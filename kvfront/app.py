import os, time, asyncio, hashlib                               #calcolo hash (md5) per distribuire le chiavi sui backend.
from typing import Any, Dict, List, Tuple, Optional

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="KV Front (Coordinator)")                   #crea l'app fastapi del kvfront


# Config da env
BACKENDS = [x.strip() for x in os.getenv("BACKENDS","http://kvstore:9000").split(",") if x.strip()]     #Legge la variabile d’ambiente BACKENDS separando 
                                                                                                        #quando trova la virgola
                                                                                                        #quindi l'output è la lista degli url dei backend
RF       = int(os.getenv("RF", "2"))                                            # fattore di replica (Rf=2)
READ_REPAIR = os.getenv("READ_REPAIR","1") == "1"                               # Flag per read-repair
HINT_FLUSH_SEC = int(os.getenv("HINT_FLUSH_SEC","2"))                           # C3: frequenza flush hint. Ogni quanti secondi provare a rispedire gli hint

if RF > len(BACKENDS):                                         #se RF maggiore del numero di backend reali, scala RF al massimo possibile  
    RF = len(BACKENDS)


# Modello request/response
class ValueModel(BaseModel):
    """
    Modello Pydantic per richieste di scrittura semplice su KV.

    Rappresenta il corpo JSON atteso dalle API `PUT /kv/{key}`.

    Attributi:
        value (Any): 
            Il valore da salvare nel KV.
            Può essere di qualsiasi tipo serializzabile in JSON 
            (stringa, numero, booleano, oggetto/dict, lista, ecc.).
    """
    value: Any

class CasModel(BaseModel):
    """
    Modello Pydantic per richieste CAS (Compare-And-Swap).

    Rappresenta il corpo JSON atteso dall’API `POST /kv/cas`.
    Serve a fare update condizionale: scrive un nuovo valore
    solo se il valore corrente coincide con quello atteso.

    Attributi:
        key (str):
            La chiave su cui effettuare la CAS (es. "delivery:123").
        old (Any):
            Il valore che il client si aspetta attualmente salvato
            (condizione di confronto).
        new (Any):
            Il nuovo valore da scrivere se la condizione è verificata.
    """
    key: str
    old: Any
    new: Any

# Util: hashing e anello
def _h(s: str) -> int:
    """
    Calcola l'hash di una stringa restituendo un intero.

    La funzione usa MD5 per ottenere un digest esadecimale e lo converte in 
    un intero base 16. Serve a trasformare una chiave arbitraria in un numero 
    deterministico da usare per il consistent hashing.

    Args:
        s (str): La stringa di input (tipicamente una chiave KV, es. "delivery:123").

    Returns:
        int: Valore intero derivato dall'hash MD5 della stringa.
    """
    return int(hashlib.md5(s.encode("utf-8")).hexdigest(), 16)              #converte la stringa in ingresso in byte, calcola l’hash MD5 e lo restituisce 
                                                                            #in esadecimale. Converte la stringa esadecimale in un intero base 16
def replica_set(key: str) -> List[str]:
    """
    Determina l'insieme di backend (repliche) da usare per una chiave.

    Usa il risultato dell'hash della chiave per scegliere un punto di partenza
    nell'elenco BACKENDS e seleziona RF backend consecutivi (primario + 
    repliche successive), con wrapping circolare se necessario.

    Args:
        key (str): La chiave da replicare (es. "delivery:123").

    Returns:
        List[str]: Lista ordinata di URL dei backend da usare.
                   - Il primo è il primario.
                   - I successivi sono le repliche.
                   - Lista vuota se non ci sono backend configurati.
    """

    if not BACKENDS:
        return []
    start = _h(key) % len(BACKENDS)                                     #usa l’hash della chiave modulo len(BACKENDS), per scegliere l’indice di partenza 
    out = []                                                            #nella lista dei BACKENDS da cui partire
    for i in range(RF):
        out.append(BACKENDS[(start + i) % len(BACKENDS)])               #costruisce la lista dei backend su cui scrivere/leggere quella chiave.
    return out


# LWW: wrapper con timestamp
def wrap(value: Any) -> Dict[str, Any]:
    """
    Incapsula un valore aggiungendo un timestamp (schema LWW).

    Ogni volta che un valore viene scritto nel KV, viene avvolto in un
    dizionario che contiene sia il dato effettivo sia il momento della
    scrittura. Questo permette di applicare la politica Last-Write-Wins
    (LWW) durante la replica o la read-repair.

    Args:
        value (Any): 
            Il valore originale da salvare (può essere stringa, dict,
            numero, ecc.).

    Returns:
        dict: Dizionario con due campi:
            - "_ts" (float): timestamp corrente (time.time()) in secondi.
            - "data" (Any): il valore originale passato in input.
    """
    return {"_ts": time.time(), "data": value}                  #prende un valore qualsiasi e restituisce un dizionario con il momento corrente e il valore

def unwrap(stored: Any) -> Tuple[float, Any]:
    """
    Estrae timestamp e valore da un elemento del KV, anche se non incapsulato.

    Normalizza la rappresentazione dei valori letti da backend: può capitare
    di ricevere un valore incapsulato dal wrapper (schema con `_ts` e
    `data`) oppure un valore non incapsulato.
    In entrambi i casi restituisce una tupla (timestamp, valore).

    Args:
        stored (Any): 
            Valore letto dal KV. Può essere:
              - dict con chiavi "_ts" e "data" (nuovo formato).
              - valore semplice (stringa, numero, dict, ecc.).

    Returns:
        tuple[float, Any]: 
            - Il timestamp associato al valore (float).
              Se non presente, restituisce 0.0.
            - Il valore effettivo.
    """
    if isinstance(stored, dict) and "_ts" in stored and "data" in stored:           #se è nel formato wrappato valido 
        return float(stored["_ts"]), stored["data"]                                 #estrae i due campi perchè è nel formato giusto
    return (0.0, stored)                                                            #se no restituisce il formato giusto con il valore e un ts molto vecchio 

                                                                                    #cosi che perde con qualsiasi confronto per LWW
# Hinted handoff
# mappa: backend_url -> lista di (key, wrapped_value)
_HINTS: Dict[str, List[Tuple[str, Dict[str, Any]]]] = {} #buffer in memoria (dict) che raccoglie le scritture non riuscite verso certe repliche.
#chiave: URL del backend in errore, valore: lista di tuple (key, wrapped_value) da ritentare più tardi.
async def flush_hints():
    """
    Loop periodico che tenta di consegnare gli "hint" (scritture non riuscite)
    alle repliche destinate.

    Funzionamento:
        - Ogni HINT_FLUSH_SEC secondi itera sulla mappa globale _HINTS.
        - Per ciascun backend, prova a reinviare tutti i (key, wrapped_value).
        - Se la PUT fallisce (HTTP != 200 o eccezione), l'item resta nel buffer.
        - Se tutti gli item di un backend vanno a buon fine, il backend viene
          rimosso da _HINTS.
    """
    while True:
        await asyncio.sleep(max(1, HINT_FLUSH_SEC))#ciclo infinito che ogni HINT_FLUSH_SEC secondi prova a svuotare il buffer.
        try:
            if not _HINTS:  #se non ci sono riscritture da fare salta 
                continue
            async with httpx.AsyncClient(timeout=2.0) as c: #crea un client http
                to_del = []
                for b, items in list(_HINTS.items()): #per ogni backends nel dizionario _HINTS
                    still: List[Tuple[str, Dict[str, Any]]] = []  #lista che raccoglierà gli elementi che ancora non riesce a scrivere 
                    for k, val in items:  #per ogni key e valore nella lista hint di quel backend prova il put sul quel backend
                        try:
                            r = await c.put(f"{b}/kv/{k}", json={"value": val})
                            if r.status_code != 200:
                                still.append((k, val)) #se non riesce la tupla finisce in still
                        except Exception:
                            still.append((k, val))
                    if still:
                        _HINTS[b] = still #se still non è vuoto aggiorna _hints con le tuple da riprovare al ciclo dopo
                    else:
                        to_del.append(b) #altrimenti segna la lista di tuple associata a b in modo da cancellarla dal buffer
                for b in to_del:
                    _HINTS.pop(b, None) #rimuove quindi la chiave dal buffer perchè non ci sono riscritture da dover fare 
        except Exception:
            # best-effort: non fermare il front
            pass

@app.on_event("startup") #All’avvio dell’app FastAPI, crea task in background che esegue flush_hints() per tutta la vita del processo.
async def _start(): 
    """
    Evento di avvio dell'app FastAPI.

    Avvia in background il task asincrono di "flush_hints" che,
    per tutta la vita del processo, proverà periodicamente a svuotare
    il buffer degli hint (_HINTS).

    Args:
        None

    Returns:
        None
    """
    # avvia il flusher di hint
    asyncio.create_task(flush_hints())

# =======================
# HTTP helpers
# =======================
async def get_one(client: httpx.AsyncClient, base: str, key: str) -> Optional[Any]:
    """
    Recupera il valore di una chiave da un singolo backend KV.

    Effettua una GET su `{base}/kv/{key}` e interpreta la risposta:
    - Se la chiave non esiste (HTTP 404) → restituisce `None`.
    - Se la richiesta ha successo (HTTP 200) → ritorna il contenuto del campo `"value"`.
    - In caso di errore di rete o eccezioni → ritorna `None`.

    Args:
        client (httpx.AsyncClient):
            Client HTTP asincrono già aperto e riutilizzabile.
        base (str):
            URL base del backend
        key (str):
            Chiave da leggere.

    Returns:
        Optional[Any]:
            - Il valore associato alla chiave (wrapped o unwrapped) se presente.
            - `None` se la chiave non esiste o il backend non è raggiungibile.
    """
    try:
        r = await client.get(f"{base}/kv/{key}") #fa una richiesta HTTP GET a all'endpoint /kv/{key} del backend specificato nell'url
        #r è la risposta HTTP del backend (status code + body JSON).
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()["value"]
    except Exception:
        return None

async def put_one(client: httpx.AsyncClient, base: str, key: str, val: Any) -> bool:
    """
    Scrive un valore su un singolo backend KV.

    Effettua una PUT su `{base}/kv/{key}` inviando il corpo JSON:
    `{ "value": val }`.
    Il valore `val` deve essere già nel formato atteso (es. wrapped LWW).

    Args:
        client (httpx.AsyncClient):
            Client HTTP asincrono già aperto e riutilizzabile.
        base (str):
            URL base del backend, es. `"http://kvstore_b:9000"`.
        key (str):
            Chiave logica da scrivere.
        val (Any):
            Valore da salvare. Può essere incapsulato (LWW) o semplice.

    Returns:
        bool:
            - `True` se il backend ha confermato con 200/201.
            - `False` in caso di errore HTTP o eccezione di rete.
    """
    try:
        r = await client.put(f"{base}/kv/{key}", json={"value": val}) #fa una richiesta HTTP PUT al backend specificato nell'URL
        #r è un codice di stato
        return r.status_code in (200, 201)

    except Exception:
        return False
    
async def _repair_many(bases: list[str], key: str, wrapped_value: dict) -> None:
    """
    Esegue un'operazione di read-repair sincronizzata su più repliche.

    Dopo una lettura con LWW, se alcune repliche risultano stantie, 
    questa funzione forza l'aggiornamento di tali repliche inviando 
    un PUT in parallelo a tutti i backend indicati.

    Args:
        bases (list[str]):
            Lista di URL dei backend da riparare.
        key (str):
            Chiave logica che deve essere aggiornata.
        wrapped_value (dict):
            Valore incapsulato in formato LWW 
            (`{"_ts": <float>, "data": <Any>}`).

    Returns:
        None.
        Tutti i risultati vengono raccolti in parallelo (asyncio.gather).
        Gli errori vengono ignorati (best-effort): 
        le repliche non raggiungibili resteranno indietro.
    """
    async with httpx.AsyncClient(timeout=2.0) as c: #Crea un client httpx temporaneo
        # lancio in parallelo ma attendo che finiscano
        tasks = [put_one(c, b, key, wrapped_value) for b in bases] #Costruisce tasks di put_one per tutti i backend da riparare
        results = await asyncio.gather(*tasks, return_exceptions=True) #esegue in parallelo e aspetta che finiscano tutti;
        #non alza eccezioni, le repliche non raggiungibili resteranno indietro
        _ = results



# =======================
# API
# =======================
@app.get("/health") # definisce endpoint HTTP GET.
def health(): 
    """
    Ritorna lo stato del front KV e parametri di configurazione.

    Returns:
        dict: Dizionario con:
            - status (str): "ok" se il processo è vivo.
            - backends (int): numero di backend configurati.
            - rf (int): replication factor effettivo (potrebbe essere ridotto rispetto all'env).
    """
    return {"status":"ok","backends":len(BACKENDS),"rf":RF}

@app.get("/kv/{key}") #definisce l'endpoint http get
async def get_key(key: str):
    """
    Legge una chiave replicata applicando LWW e fa read-repair best-effort.

    Strategia:
        - Raccoglie i valori da tutte le repliche del replica set della chiave.
        - Sceglie il più recente usando LWW (confronto su timestamp interno "_ts").
        - (Opz.) Esegue read-repair sulle repliche stantie che hanno risposto.

    Args:
        key (str): Chiave logica da leggere.

    Returns:
        dict: {"key": <key>, "value": <best_value>} dove <best_value> è il valore
        "unwrapped" (senza metadati LWW).

    
    """
    reps = replica_set(key) #calcola primario+secondari per la chiave chiamando la funzione responsabile 
    if not reps:
        raise HTTPException(503, "No backends") #alza l'errore se non crova un replica set
    async with httpx.AsyncClient(timeout=2.0) as c: #crea un client http
        vals = await asyncio.gather(*[get_one(c, b, key) for b in reps])  #legge in sincrono tutte le repliche tramite l'helper get_one avviando tante coroutine in parallelo
        # l' * è per passare gli elementi della lista uno a uno 
        #vals è una lista, uno per replica, che può contenere il valore wrappato o none (se la replica non ha la chiave)
    # scegli il più recente (LWW)
    best_ts, best_val, best_idx = -1.0, None, -1
    for i, v in enumerate(vals): #scorre tra le repliche
        if v is None: 
            continue #scarta le None
        ts, data = unwrap(v) #estrae time stamp e valore
        if ts > best_ts:
            best_ts, best_val, best_idx = ts, data, i #identifica il valore più nuovo quindi con il ts più alto (bestval valore da restituire al client)

    if best_idx < 0:
        raise HTTPException(404, "Key not found")

    # C2: read-repair: aggiorna repliche non allineate (best effort)
    if READ_REPAIR and best_ts >= 0: #best_ts >= 0 vuold ire che è stato trovato trovato almeno una replica valida
        wrapped = {"_ts": best_ts, "data": best_val} #Ricostruisce il valore giusto
        to_fix = []
        for i, b in enumerate(reps): #scorre le repliche
            v = vals[i] #guardiamo il valore corrente sulla replica 
            ts = unwrap(v)[0] if v is not None else -1.0 #estrae il timestamp di quella replica.
            # Ripariamo solo repliche che hanno risposto (ts >= 0) ma sono vecchie
            if ts >= 0 and ts < best_ts: #ts < best_ts: il suo valore è più vecchio del migliore trovato.
                to_fix.append(b) #aggiungiamo la replica in quelle da riparare
        if to_fix:
            await _repair_many(to_fix, key, wrapped)  #ripara le repliche stantie
    return {"key": key, "value": best_val}

@app.put("/kv/{key}")
async def put_key(key: str, body: ValueModel):
    """
    Scrive un valore replicato con LWW e hinted handoff.

    Strategia:
        - Calcola il replica set (primario + secondari) per la chiave.
        - Incapsula il valore con timestamp (wrap) per LWW.
        - Prova a scrivere su tutte le repliche:
            * se una replica fallisce → accoda un "hint" da flushare periodicamente.
        - Risponde OK se almeno 1 replica ha scritto (sloppy quorum via hints).

    Args:
        key (str): Chiave logica da scrivere.
        body (ValueModel): JSON con campo "value" (qualsiasi JSON serializzabile).

    Returns:
        dict: {"ok": True, "written": <n_ok>, "rf": <RF>}

    
    """
    reps = replica_set(key)  #estrae il replica set per quella chiave 
    if not reps:
        raise HTTPException(503, "No backends") #se non le trova solleva errore

    wrapped = wrap(body.value) #incapsula il valore della chiave con il timestamp
    ok = 0 #Contatore di quante repliche hanno accettato la scrittura.
    async with httpx.AsyncClient(timeout=2.0) as c: #crea un client http
        for b in reps:  #itera su tutti i backend nel replica set
            if await put_one(c, b, key, wrapped): #tenta di fare il put se va bene incrementa ok
                ok += 1
            else:#se non riesce 
                # salva hint per backend b
                _HINTS.setdefault(b, []).append((key, wrapped)) #.setdefault:Se _HINTS già contiene una lista per il backend b, la restituisce.
                                                                 #Se invece b non è ancora presente, la aggiunge con valore [] (lista vuota) e poi restituisce quella lista.
                                                                #Alla lista trovata o creata per b, aggiunge il nuovo elemento
    if ok == 0:
        raise HTTPException(503, "Write failed on all replicas")
    # Nota: per Rf=2, rispondiamo OK anche con 1 replica (sloppy quorum via hint)
    return {"ok": True, "written": ok, "rf": RF}

@app.post("/kv/cas")
async def cas(body: CasModel):
    """
    Esegue una Compare-And-Swap (CAS) sulla chiave, consistente col primario.

    Strategia:
        1) Determina primario e secondari per la chiave.
        2) Legge il valore corrente dal primario (wrapped) e lo "unwrap".
        3) Confronta lato front: se "old" del client ≠ stato attuale → fail.
        4) Prepara "new" wrapped e invia una CAS reale al primario:
           - only-if (old_wrapped == current_wrapped) lato backend.
        5) Se il primario conferma → replica best-effort sui secondari
           (o accoda hint se down).

    Args:
        body (CasModel): JSON con campi "key", "old", "new".

    Returns:
        dict:
          - {"ok": True} se la CAS va a buon fine sul primario.
          - {"ok": False, "current": <val>} se fallisce (con value corrente unwrapped).

    
    """
    reps = replica_set(body.key)  #determina il replica set della chiave  (body è un oggetto CASModel quindi è fatto cosi:{ "key": "...", "old": <val>, "new": <val> })
    if not reps:
        raise HTTPException(503, "No backends") #se non trova solleva l'errore
    primary, secondaries = reps[0], reps[1:] #salva in primaries il primo elemento di reps e in secondaries tutti gli altri

    async with httpx.AsyncClient(timeout=2.0) as c:#crea un client
        
        cur_raw = await get_one(c, primary, body.key)  #leggo il VALORE CORRENTE (wrapped) dal primario
        cur_unwrapped = unwrap(cur_raw)[1] if cur_raw is not None else None  #estrae solo il dato ignorando il timestamp.

        # confronto lato front: se l'OLD richiesto dal client non coincide con lo stato attuale, fallisco
        if cur_unwrapped != body.old: #se il valore estratto  è diverso da quello vecchio salvato nell'oggetto body
            return {"ok": False, "current": cur_unwrapped}  #al client arriva l'ultimo valore effettivo della chiave

      
        new_wrapped = wrap(body.new) #Prepara il nuovo valore, incapsulato con timestamp

        #CAS reale sul primario:  front-end KV non fa più controlli da solo, ma chiede al backend primario di eseguire la CAS.
        #Perché solo il backend sa se nel frattempo qualcun altro ha scritto sulla stessa chiave.
        r = await c.post(f"{primary}/kv/cas", json={
            "key": body.key,  #la chiave da aggiornare
            "old": cur_raw,     #valore WRAPPED intero letto poco prima dal primario che però tra il tempo di lettura e scrittura potrebbe essere stato cambiato da un altro client 
            "new": new_wrapped   #nuovo valore wrapped che vogliamo scrivere
        })  #r è la risposta HTTP dal backend primario
        r.raise_for_status()
        resp = r.json()  #trasformiamo la risposta in un dizionario python che ha la chiave ok (valore:true/false) e la chiave current (valore: il dato wrappato)
        if not resp.get("ok"): #se fallisce il front riporta al client il valore attuale che ha vinto
            
            current_backend = resp.get("current") #restituisce il valore del campo current della risposta
            current_unwrapped = unwrap(current_backend)[1] if current_backend is not None else None #estrae solo il dato logico senza ts
            return {"ok": False, "current": current_unwrapped} #risponde al client

        
        for b in secondaries:#Se il primario ha accettato la CAS, il nuovo valore va replicato anche sui secondari.
            if not await put_one(c, b, body.key, new_wrapped): #prova put_one se non va accoda la key e il valore in _HINTS per quel backend
                _HINTS.setdefault(b, []).append((body.key, new_wrapped))

    return {"ok": True}





# Lock: inoltriamo al primario (semplice, coerente col PoC)
@app.post("/lock/acquire/{key}") #definisce endpoint post
async def lock_acquire(key: str, ttl_sec: int = 30):
    """
    Acquisisce un lock distribuito per 'key' inoltrando la richiesta al primario.

    Args:
        key (str): Nome del lock.
        ttl_sec (int, opzionale): TTL del lock in secondi (default 30).

    Returns:
        dict: Risposta JSON del backend primario (es. {"ok": true/false, ...}).

    
    """
    reps = replica_set(f"lock:{key}")  #trova il replica set della key su cui fare il lock (non è il documento della delivery ma un oggetto del kv che contiene lo stato del lock e la key della delivery associata)
    if not reps:
        raise HTTPException(503, "No backends")
    primary = reps[0] #prende il primario 
    async with httpx.AsyncClient(timeout=2.0) as c: #crea il client 
        try:
            r = await c.post(f"{primary}/lock/acquire/{key}", params={"ttl_sec": ttl_sec}) #invia una richiesta all'endpoint post 
            return r.json() #prende la risposta e la converte in json e la restituisce. Se trova il campo ttl già inserito fallisce il lock
        except Exception:
            raise HTTPException(503, "Lock backend unavailable")

@app.post("/lock/release/{key}") #Definisce l’endpoint HTTP POST su /lock/release/<key>.
async def lock_release(key: str):
    """
    Rilascia un lock distribuito inoltrando la richiesta al primario.

    Args:
        key (str): Nome logico del lock.

    Returns:
        dict: Risposta JSON del backend primario.

    """
    reps = replica_set(f"lock:{key}") ##trova il replica set della key su cui fare il lock (non è il documento della delivery ma un oggetto del kv che contiene lo stato del lock e la key della delivery associata)
    if not reps:
        raise HTTPException(503, "No backends")
    primary = reps[0]
    async with httpx.AsyncClient(timeout=2.0) as c: #crea il client http
        try:
            r = await c.post(f"{primary}/lock/release/{key}")#Fa una richiesta POST verso il backend primario sull’endpoint /lock/release/<key>.
            return r.json()  #restituisce sempre la risposta
        except Exception:
            raise HTTPException(503, "Lock backend unavailable")