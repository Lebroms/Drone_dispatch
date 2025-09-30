import os, json, uuid, asyncio, time        #os=per leggere variabili d’ambiente json=per convertire oggeti python in json
                                            #uuid=per generare ID univoci delle delivery  
                                            #asyncio=per task asincroni  time= per timestamp
from typing import Optional, Any
from pathlib import Path

from fastapi import FastAPI                 #classe fornita dal framework FastAPI con cui si costruisce un applicazione web che poi verrà eseguita da Uvicorn
from fastapi import HTTPException           #per risposte HTTP con codice/errore.
from fastapi import Request, Response       #servono affinche il gateway possa interagire con le richieste e le risposte http, gestiscono headers, parametri e corpi dei messaggi
from pydantic import BaseModel, Field

import aio_pika                             #permette al gateway di parlare con Rabbit in modo asincrono. Serve quindi per creare un client AMQP (protocollo di rabbit) asincrono
import httpx                                #per creare client http

# import per servire file statici / dashboard
from fastapi.staticfiles import StaticFiles                     # per rendere disponibili file statici            
from fastapi.responses import HTMLResponse, FileResponse        #serve per dire che il contenuto di una riposta è html, serve per fare una risposta http che restituisce un file

RABBIT_URL = os.getenv("RABBIT_URL","amqp://guest:guest@rabbitmq/")     #url del broker
KV_URL     = os.getenv("KV_URL","http://kvfront:9000")                  #url del kvfront

DELIVERY_REQ_QUEUE    = os.getenv("DELIVERY_REQ_QUEUE", "delivery_requests")        #coda dove il gateway pubblica le nuove delivery
DELIVERY_STATUS_QUEUE = os.getenv("DELIVERY_STATUS_QUEUE", "delivery_status")       #coda dove il gateway consuma gli aggiornamenti delle delivery 

# ----------- Rettangolo e griglia delle zone (Roma) -----------
RECT_LAT_MIN = 41.80
RECT_LAT_MAX = 41.98
RECT_LON_MIN = 12.37
RECT_LON_MAX = 12.60                                #buonds su cui creare la griglia di zone
GRID_ROWS = int(os.getenv("GRID_ROWS", "2"))        #righe della griglia
GRID_COLS = int(os.getenv("GRID_COLS", "2"))        #colonne della griglia

def build_zones_config():
    """
    Costruisce la configurazione delle zone geografiche.

    - Divide il rettangolo definito dalle costanti globali (RECT_LAT/LON_*)
      in una griglia di GRID_ROWS x GRID_COLS celle.
    - Per ciascuna cella crea un dizionario con:
        * name: nome univoco (es. "zone 0")
        * row, col: coordinate di griglia
        * bounds: lat_min, lat_max, lon_min, lon_max
        * charge: punto centrale usato come stazione di ricarica
        * neighbors: vicini ortogonali (su, giù, sinistra, destra)
    - Costruisce anche la mappa globale di adiacenze.

    Returns:
        dict: Configurazione completa:
              - "bounds": bounding box globale
              - "rows": numero di righe
              - "cols": numero di colonne
              - "zones": lista di zone
              - "adjacency": mappa zona→vicini
    """

    lat_span = RECT_LAT_MAX - RECT_LAT_MIN  #ampiezza dell'area totale in latitudine 
    lon_span = RECT_LON_MAX - RECT_LON_MIN  #ampiezza dell'area totale in longitudine
    lat_step = lat_span / GRID_ROWS         #altezza di una riga della griglia
    lon_step = lon_span / GRID_COLS         #ampiezza di una colonna della griglia 

    zones = []                              #lista vuota che conterrà i dizionari delle zone
    def name(r, c):
        '''
        Calcola il nome univoco di una zona a partire dalle coordinate
        di riga e colonna nella griglia.

        Args:
            r (int): indice di riga della cella (0 <= r < GRID_ROWS).
            c (int): indice di colonna della cella (0 <= c < GRID_COLS).

        Returns:
            str: Nome della zona in formato "zone {indice}", dove
                
        '''
        
        idx = r * GRID_COLS + c
        return f"zone {idx}"     

    # celle
    for r in range(GRID_ROWS):                          #loop sul numero di righe della griglia (parte da 0)
        lat_top = RECT_LAT_MAX - r*lat_step
        lat_bottom = RECT_LAT_MAX - (r+1)*lat_step      #calcolo bordi latitudinali della singola cella 
        lat_min = min(lat_top, lat_bottom)
        lat_max = max(lat_top, lat_bottom)
        for c in range(GRID_COLS):                      #loop sul numero di righe della griglia (parte da 0)
            lon_left = RECT_LON_MIN + c*lon_step
            lon_right = RECT_LON_MIN + (c+1)*lon_step   #calcolo bordi longitudinali della singola cella 
            lon_min = min(lon_left, lon_right)
            lon_max = max(lon_left, lon_right)
            zones.append({
                "name": name(r,c),
                "row": r, "col": c,
                "bounds": {"lat_min": lat_min, "lat_max": lat_max,
                           "lon_min": lon_min, "lon_max": lon_max},
                "charge": {"lat": (lat_min+lat_max)/2.0, "lon": (lon_min+lon_max)/2.0},
                "neighbors": []
            }) #costruzione della singola zona con i bound e con le coordinate del punto di ricarica di quella zona 

    # adiacenze 4-neighbors
    for z in zones:                                                     #itera le zone
        r, c = z["row"], z["col"]                                       #estrae la riga e la colonna della zone nella griglia 
        neigh = []
        for dr, dc in [(-1,0),(1,0),(0,-1),(0,1)]:                      #(sopra sotto sinistra destra)
            rr, cc = r+dr, c+dc
            if 0 <= rr < GRID_ROWS and 0 <= cc < GRID_COLS:             #calcolo vicini
                neigh.append(f"zone {rr*GRID_COLS+cc}")
        z["neighbors"] = neigh                                          #salva la lista dei vicini nel dizionario di quella zona 

    adjacency = { z["name"]: z["neighbors"] for z in zones }            #crea una mappa delle zone nome+lista vicini
    return {
        "bounds": {"lat_min": RECT_LAT_MIN, "lat_max": RECT_LAT_MAX,
                   "lon_min": RECT_LON_MIN, "lon_max": RECT_LON_MAX},
        "rows": GRID_ROWS, "cols": GRID_COLS,
        "zones": zones,
        "adjacency": adjacency
    }    #restituisce la configurazione completa delle zone

def point_zone(zcfg, p):
    """
    Determina in quale zona ricade un punto geografico.

    Args:
        zcfg (dict): Configurazione delle zone (come da build_zones_config()).
        p (dict): Punto con chiavi {"lat": float, "lon": float}.

    Returns:
        str | None: Il nome della zona in cui cade il punto,
                    oppure None se non appartiene a nessuna zona.
    """
    for z in zcfg["zones"]:
        b = z["bounds"]
        if b["lat_min"] <= p["lat"] <= b["lat_max"] and b["lon_min"] <= p["lon"] <= b["lon_max"]:
            return z["name"]
    return None

# ====== App ======
app = FastAPI(title="Gateway")                      #oggetto della classe fastapi che sa ricevere richieste HTTP, instradarle alle funzioni che
                                                    #che vengono definite, restituire risposte.

_static_dir = Path(__file__).parent / "static"     #__file__= variabile speciale di Python che contiene il percorso del file corrente
                                                   #con path creo un oggetto python che punta al percorso contenuto in quella variabile
                                                   #sull'oggetto path .parent restituisce la directory corrente 
                                                   #usa il / per unire i percorsi. Quindi static_dir oggetto python che rappresenta la cartella static


if _static_dir.is_dir():                                                            #se esiste e è una directory    
                                                                                    # montare un file server sotto il path /static, cosi ogni richiesta che inizia con /static/... viene gestita direttamente da StaticFiles”                       
    app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")    #Quando arriva una richiesta HTTP a /static
                                                                                    #FastAPI non la gestisce con una funzione @app.get.
                                                                                    #Passa la richiesta a StaticFiles.
                                                                                    #StaticFiles apre i file dal disco (dentro la cartella che gli ha) e li manda al client 
                                                                            
# ora /static/dashboard.html è servito dal web server

#@= prende la funzione subito sotto e lo passa a get come argomento
@app.get("/dashboard", response_class=HTMLResponse)                                 #crea la rotta per le richieste http all'url dashboard e la risposta che viene restituita deve essere html
                                                                                    #quando avviene una richiesta la inoltra alla funzione dashboard()
async def dashboard():                                                              #non riceve valori dalla request
    """Serve la dashboard se presente; altrimenti un 404"""
    html_path = _static_dir / "dashboard.html"
    if html_path.exists():
        return FileResponse(html_path)                                              #se il path esiste restiuisce il file 
    return HTMLResponse(
        "<h1>Dashboard non trovata</h1><p>Manca <code>static/dashboard.html</code> nell'immagine del gateway.</p>",
        status_code=404                                                             #corpo di risposta html per dire che la dashbaord non è raggiungibile 
    )

# ====== Stato connessioni ======
amqp_conn: aio_pika.RobustConnection | None = None
amqp_channel: aio_pika.Channel | None = None
status_consumer_task: asyncio.Task | None = None
http_client: httpx.AsyncClient | None = None                 #servono a definire lo stato globale delle connessioni del gateway

# ====== Schemi ======
class Point(BaseModel):                                      #basemodel classe base da cui derivano tutti i modelli pydantic che descrive campi tipizzati aggiungendo vincoli di validazione
    """
    Modello Pydantic che rappresenta un punto geografico.

    Questo schema è usato per indicare la posizione di origine e
    destinazione di una consegna. Ogni punto è espresso in coordinate
    geografiche (latitudine e longitudine) e viene validato per rientrare
    nei range standard del globo terrestre.

    Attributes:
        lat (float): Latitudine in gradi decimali.
            - Deve essere compresa tra -90 e +90.
            - Valori negativi indicano l'emisfero sud.
        lon (float): Longitudine in gradi decimali.
            - Deve essere compresa tra -180 e +180.
            - Valori negativi indicano l'emisfero ovest.

    """
    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=180)

class DeliveryRequest(BaseModel):
    """
    Modello che descrive la richiesta di una nuova consegna.
    Attributes:
        origin (Point): Coordinate del punto di pickup (lat/lon).
        destination (Point): Coordinate del punto di consegna (lat/lon).
        weight (float): Peso del pacco in kg. Deve essere > 0.

    """
    origin: Point
    destination: Point
    weight: float = Field(..., gt=0)

class DeliveryStatus(BaseModel):
    """
    Modello di risposta usato dalle API del gateway per riportare lo stato attuale di una consegna.

    Attributes:
        id (str): Identificativo univoco della delivery (UUID in formato stringa).
        status (str): Stato attuale della consegna.
            Valori attesi:
              - "pending"     : richiesta ricevuta, non ancora assegnata a un drone
              - "assigned"    : assegnata a un drone, in trasferimento verso il pickup (leg = to_origin)
              - "in_flight"   : in corso; dal primo movimento rilevato
              - "delivered"   : consegna completata
        drone_id (Optional[str]): Identificativo del drone assegnato.
            - Presente tipicamente da "assigned" in poi.
            - Può essere None quando la delivery è ancora "pending".

    Notes:
        Questo schema è pensato per l’OUTPUT delle API del gateway.
        I valori sono letti dal KV e riflessi nella risposta HTTP.
    """
    id: str
    status: str
    drone_id: Optional[str] = None

# ====== Helper KV (usano l'http_client globale, inizializzato allo startup)======
async def kv_put(key:str, value:dict):
    """
    Scrive un valore nel key-value store (KV).

    Args:
        key (str): Nome della chiave (es. 'delivery:<id>', 'zones_config').
        value (dict): Valore da salvare sotto la chiave. Verrà incapsulato
                      in un JSON come {"value": value}.

    Returns:
        None

    """
    if not http_client: return
    try:
        await http_client.put(f"/kv/{key}", json={"value": value})              #esegue una put sul kv con body json 
    except Exception:
        pass

async def kv_get(key:str) -> dict:
    """
    Legge un valore dal key-value store (KV).

    Args:
        key (str): Nome della chiave da leggere.

    Returns:
        dict: Il contenuto della chiave (campo 'value').

    Raises:
        HTTPException: 
            - 503 se il client HTTP non è ancora pronto.
            - 404 se la chiave non esiste nel KV.
    """
    if not http_client: raise HTTPException(503, "KV not ready")
    r = await http_client.get(f"/kv/{key}")                                     #Esegue una GET su /kv/{key}
    if r.status_code == 404:
        raise HTTPException(404, "Not found")
    return r.json()["value"]

async def kv_get_opt(key:str):                                                  #altra versione meno stringente del get 
    """
    Legge un valore dal key-value store (KV), senza sollevare eccezioni.

    Args:
        key (str): Nome della chiave da leggere.

    Returns:
        dict | None: 
            - Il valore della chiave se esiste.
            - None se la chiave non esiste o se il client HTTP non è pronto.

    """
    if not http_client: return None
    r = await http_client.get(f"/kv/{key}")
    if r.status_code == 404:
        return None
    return r.json()["value"]

async def kv_cas(key:str, old:Any, new:Any) -> bool:
    """
    Esegue un'operazione Compare-And-Swap (CAS) sul KV.

    Args:
        key (str): Chiave da aggiornare.
        old (Any): Valore atteso corrente. L'update avverrà solo se
                   il valore effettivo nel KV coincide con questo.
        new (Any): Nuovo valore da scrivere se il confronto ha successo.

    Returns:
        bool: 
            - True se l'update è stato eseguito con successo.
            - False se il valore attuale non coincide con 'old'
              o in caso di errore/timeout del KV.

    """
    if not http_client: return False
    try:
        r = await http_client.post("/kv/cas", json={"key":key,"old":old,"new":new})
        return bool(r.json().get("ok"))
    except Exception:
        return False

# ====== Rabbit helpers ======
async def ensure_rabbit_channel():
    """
    Garantisce che esista una connessione e un canale AMQP valido verso RabbitMQ.

    - Se esiste già un canale aperto (`amqp_channel`), lo restituisce subito.
    - Se manca o è chiuso, crea (o ricrea) la connessione `amqp_conn` e un nuovo canale.
    - Assicura che le code `DELIVERY_REQ_QUEUE` e `DELIVERY_STATUS_QUEUE`
      siano dichiarate (idempotente, `durable=True`).

    Returns:
        aio_pika.Channel | None: un canale pronto da usare,
        oppure `None` se non è stato possibile stabilire la connessione.

    """
    global amqp_conn, amqp_channel                                                      #Usa le variabili globali per riutilizzare connessione e canale tra le varie chiamate.
    try:
        if amqp_channel and not amqp_channel.is_closed:
            return amqp_channel
        if amqp_conn is None or amqp_conn.is_closed:
            amqp_conn = await aio_pika.connect_robust(RABBIT_URL)                       #crea una connessione (tcp tra client e rabbit) robusta nel caso in cui non ci sia
        amqp_channel = await amqp_conn.channel()                                        #Apre un nuovo canale (oggetto su cui pubblicano o consumano messaggi) sulla connessione
        await amqp_channel.declare_queue(DELIVERY_REQ_QUEUE, durable=True)
        await amqp_channel.declare_queue(DELIVERY_STATUS_QUEUE, durable=True)           #operazioni idempotenti: Se la coda non esiste ancora sul broker, la crea 
                                                                                        #Se invece esiste già, non la ricrea e non genera errore
        return amqp_channel  
    except Exception as e:
        print("[gateway] WARN ensure_rabbit_channel:", e)
        return None

async def publish_delivery_request(payload: dict):
    """
    Pubblica un nuovo evento di richiesta consegna su RabbitMQ.

    Parametri:
        payload (dict): dizionario JSON serializzabile con i dettagli della consegna.
            Tipico contenuto:
                {
                    "delivery_id": <str>,
                    "origin": {"lat": float, "lon": float},
                    "destination": {"lat": float, "lon": float},
                    "weight": <float>
                }

    Returns:
        None
    """
    ch = await ensure_rabbit_channel()                                              #cerca di ottenere un canale per parlare con le code 
    if not ch:
        print("[gateway] WARN: Rabbit not ready, skip publish")
        return
    try:
        msg = aio_pika.Message(
            body=json.dumps(payload).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json"
        )                                                                           #creazione messaggio AMQP
        await ch.default_exchange.publish(msg, routing_key=DELIVERY_REQ_QUEUE)      #Pubblica sull’exchange di default con routing key

    #blocco con cui prova a pubblicare una seconda volta nel caso sia fallita la prima 
    except Exception as e:
        print("[gateway] WARN publish failed once, retry:", e)
        ch = await ensure_rabbit_channel()
        if ch:
            try:
                await ch.default_exchange.publish(msg, routing_key=DELIVERY_REQ_QUEUE)
            except Exception as e2:
                print("[gateway] ERROR publish failed again:", e2)

async def run_status_consumer():
    """
    Avvia un consumer asincrono per la coda `DELIVERY_STATUS_QUEUE`. Consuma i dati dalla coda senza inviarli a nessun altro servizio.

    Funzionamento:
        - Gira in un loop infinito all’interno di un task di background.
        - Usa `ensure_rabbit_channel()` per mantenere una connessione valida.
        - Consuma i messaggi da `delivery_status` con ack automatico.
        - Decodifica il corpo come JSON e lo stampa a log:
          `[gateway] delivery_status: { ... }`.
        - In caso di errori di connessione o parsing, logga e continua.

    Returns:
        None (loop infinito).

    """
    backoff = 1.0
    while True: 
        try:
            ch = await ensure_rabbit_channel()                                      #prova ad avere un canale; se non c’è, aspetta con backoff
            if not ch:
                await asyncio.sleep(backoff)
                backoff = min(2.0, backoff*1.5)
                continue
            queue = await ch.declare_queue(DELIVERY_STATUS_QUEUE, durable=True)     #Assicura l’esistenza della coda delivery_status (cioè se non c'è la crea)

            async with queue.iterator() as qit:                                     #iteratore asincrono sui messaggi della coda.
                backoff = 1.0
                async for message in qit:                                           #itera sui messaggi che arrivano
                    async with message.process(ignore_processed=True):              #per ogni messaggio che itera prova a mandare gli ack verso rabbit
                        try:
                            data = json.loads(message.body.decode())
                            print("[gateway] delivery_status:", data)
                        except Exception:
                            pass
        except Exception as e:
            print("[gateway] consumer reconnecting:", e)
            await asyncio.sleep(backoff)
            backoff = min(2.0, backoff*1.5)

# ====== Startup / Shutdown ======
@app.on_event("startup")    #appena l'applicazione creata con fastapi parte viene eseguita questa istruzione che tramite @ ha in ingresso la callback subito sotto
async def startup():
    """
    Evento di avvio dell'app FastAPI.

    - Crea il client HTTP globale (`http_client`) per comunicare con il KV.
      · base_url = KV_URL (default: "http://kvfront:9000")
      · timeout = 5 secondi
    - Verifica la presenza di "zones_config" nel KV:
      · se non c’è, la ricostruisce con `build_zones_config()` e la salva.
    - Avvia in background il consumer `run_status_consumer()`:
      · legge dalla coda AMQP "delivery_status"
      · logga gli aggiornamenti ricevuti
      · si riconnette da solo in caso di errore.
    Non blocca lo startup in caso di errore sul KV o su Rabbit.
    """
    global http_client, status_consumer_task                            #Usa le variabili globali prima definite 
    print("[gateway] startup…")
    http_client = httpx.AsyncClient(base_url=KV_URL, timeout=5.0)       #creaazione del client HTTP asincrono verso il KV.
                                                                        #garantisce zones_config (senza fallire lo startup)
    z = await kv_get_opt("zones_config")                                #legge la chiave zones config dal kv
    if not z:
        await kv_put("zones_config", build_zones_config())              #se non esiste la costruisce chiamando la funzione apposita

    # avvia consumer in background, non blocca e si riconnette da solo
    status_consumer_task = asyncio.create_task(run_status_consumer())  #Crea una task asincrona che esegue run_status_consumer() (ovvero il consumer che legge da delivery status)

@app.on_event("shutdown")   #appena l'applicazione creata con fastapi viene chiusa questa istruzione viene eseguita che tramite @ ha in ingresso la callback subito sotto
async def shutdown():
    """
    Evento di spegnimento dell'app FastAPI.

    - Cancella in modo sicuro il task di background `status_consumer_task`
      (consumer della coda "delivery_status").
    - Chiude il client HTTP globale (`http_client`).
    - Chiude la connessione AMQP (`amqp_conn`) se ancora aperta.
    - Logga il completamento dello shutdown.
    
    """
    global http_client, amqp_conn               #usa le variabili globali prima definite 
                                                #blocco per chiudere il consumer
    if status_consumer_task:
        status_consumer_task.cancel()
        try:
            await status_consumer_task
        except Exception:
            pass
                                                #blocco per chiudere il client http
    if http_client:
        await http_client.aclose()
                                                #blocco per chiudere connessioni aperte 
    if amqp_conn and not amqp_conn.is_closed:
        try:
            await amqp_conn.close()
        except Exception:
            pass
    print("[gateway] shutdown complete")

# ====== Health / Zones ======                            
@app.get("/health")         #Definisce l’endpoint: metodo GET, percorso /health. Consente di verificare che il gateway risponda. 
async def health(): 
    """
    Endpoint di health-check per verificare che il servizio Gateway sia attivo.

    Returns:
        dict: Dizionario JSON con un singolo campo:
            - "status" (str): Sempre "ok" se il servizio risponde correttamente.
    """
    return {"status":"ok"}


                                #ottenere (e se manca, creare) la configurazione delle zone geografiche usate da tutto il sistema.
@app.get("/zones")              #Definisce l’endpoint: metodo GET, percorso /zones.
async def get_zones():
    """
    Endpoint che restituisce la configurazione delle zone geografiche gestite dal sistema.
    Se manca nel KV, la ricrea in tempo reale e la salva.

    Returns:
        dict: Oggetto JSON con i seguenti campi principali:
            - "bounds": Limiti geografici complessivi (lat_min, lat_max, lon_min, lon_max).
            - "rows" (int): Numero di righe della griglia.
            - "cols" (int): Numero di colonne della griglia.
            - "zones" (list[dict]): Elenco di zone, ciascuna con:
                - "name" (str): Nome univoco (es. "zone 0").
                - "bounds" (dict): Limiti geografici della cella.
                - "charge" (dict): Coordinate della stazione di ricarica della zona.
                - "neighbors" (list[str]): Nomi delle zone adiacenti.
            - "adjacency": Mappa globale zona → vicini.
    """
    
    zcfg = await kv_get_opt("zones_config")         #prova a prenderla dal kv 
    if not zcfg:                                    #se non c'è la ricrea in tempo reale
        zcfg = build_zones_config()
        await kv_put("zones_config", zcfg)
        print("[gateway] zones_config (re)created on-demand")
    return zcfg

# ====== API deliveries/drones ======
@app.post("/deliveries", response_model=DeliveryStatus, status_code=201)                #Definisce l’endpoint: metodo POST, percorso /deliveries.
async def create_delivery(req: DeliveryRequest, request: Request, response: Response):
    """
    Crea una nuova delivery e pubblica un evento su RabbitMQ per l'assegnazione.

    Flusso:
      1) (Opzionale) Gestione Idempotency-Key per evitare duplicati in caso di retry client.
      2) Creazione/aggiornamento di `zones_config` se assente.
      3) Scrittura di `delivery:{id}` nel KV con stato `pending`.
      4) Inserimento di `delivery_id` in `deliveries_index` (CAS + retry).
      5) Publish su coda AMQP `delivery_requests` (best-effort con 1 retry).

    Args:
        req (DeliveryRequest): Corpo della richiesta con origin, destination e peso (>0).
        request (Request): Oggetto FastAPI per leggere header come `Idempotency-Key`.
        response (Response): Oggetto FastAPI per impostare lo status code (200 in caso di idempotenza).

    Returns:
        DeliveryStatus: JSON con `id`, `status` e `drone_id` 
    """
    # --- Idempotency-Key ---
    idem_key = request.headers.get("Idempotency-Key")                                   #Legge l’header Idempotency-Key della richiesta creata da ordergen
    if idem_key:                                                                        #se c'è
        existing = await kv_get_opt(f"idem:{idem_key}")                                 #cerca la chiave nel kv quindi existing è la delivery_id associata a quella chiave
        if existing:                                                                    #se esiste il valore associato all'idem-key, significa che già in passato è stata usata questa Idempotency-Key, e c’è scritto quale delivery_id era stato generato la prima volta.
            d = await kv_get_opt(f"delivery:{existing}")                                #qui recupero il delivery_id corrispondente
            if d:
                response.status_code = 200
                return d
            delivery_id = existing
        else:                                                                           #se non esiste
            tentative_id = str(uuid.uuid4())                                            #genera un nuovo id 
            if not await kv_cas(f"idem:{idem_key}", None, tentative_id):                #prova il CAS (solo uno riesce ovvero il primo backend che arriva) se fallisce vuol dire che qualcun altro ci è riuscito per primo
                winner = await kv_get_opt(f"idem:{idem_key}")                           #recupero la delivery id di chi ha vinto 
                d = await kv_get_opt(f"delivery:{winner}") if winner else None
                if d:
                    response.status_code = 200
                    return d
                delivery_id = winner or tentative_id
            else:                                                                       #se invece la CAS va a buon fine setto il tentive_id come delivery_id
                delivery_id = tentative_id
    else:                                                                               #se non c'è idem_key 
        delivery_id = str(uuid.uuid4())                                                 #crea un nuovo id 

    zcfg = await kv_get_opt("zones_config") or build_zones_config()                     #legge o costruisce la configurazione delle zone
    await kv_put("zones_config", zcfg)                                                  #idempotente (se l'ha creata la deve anche scrivere ma se invece già c'era non fa nulla)

    oz = point_zone(zcfg, req.origin.model_dump())  
    dz = point_zone(zcfg, req.destination.model_dump())                                 #estrae dalla richiesta i campi origin e destination per calcolare in che zona ricadono

    await kv_put(f"delivery:{delivery_id}", {                                           #scrive la delivery sul kv 
        "id": delivery_id,
        "status": "pending",
        "drone_id": None,
        "origin": req.origin.model_dump(),
        "destination": req.destination.model_dump(),
        "weight": req.weight,
        "origin_zone": oz,
        "destination_zone": dz,
        "timestamp": time.time()
    }) 

                                                                                        #deliveries_index (CAS idempotente, con backoff leggero)
    created = await kv_cas("deliveries_index", None, [delivery_id])                     #prova a scrivere delivery id in deliveries index
    if not created:                                                                     #se non riesce
        for _ in range(40):
            cur = await kv_get_opt("deliveries_index") or []
            if delivery_id in cur:
                break                                                                   #controlla se è gia presenta e se si esce
            if await kv_cas("deliveries_index", cur, cur + [delivery_id]):
                break                                                                   #riprova il cas e se va esce se no ci rientra nel for 
            await asyncio.sleep(0.05)

    # --- publish evento (lazy + retry interno) ---
    await publish_delivery_request({                                                    #Pubblica l’evento su RabbitMQ                                         
        "delivery_id": delivery_id,
        "origin": req.origin.model_dump(),
        "destination": req.destination.model_dump(),
        "weight": req.weight
    })
    print(f"[gateway] NEW delivery id={delivery_id} weight={req.weight} - published (or queued later if Rabbit not ready)")
    return DeliveryStatus(id=delivery_id, status="pending")                             #restituisce la risposta che arriverà al client

@app.get("/deliveries/{delivery_id}", response_model=DeliveryStatus)                #definisce l'endpoint specificando anche il modello di risposta da usare
async def get_delivery(delivery_id:str):                                            #la callback eseguita quando arriva una richiesta del tipo definito nell'endpoint 
    """
    Restituisce lo stato sintetico di una delivery.

    Args:
        delivery_id (str): Identificativo della delivery (UUID string).

    Returns:
        DeliveryStatus: Oggetto con `id`, `status` corrente e `drone_id` (se assegnata).
    """
    d = await kv_get(f"delivery:{delivery_id}")                                     #legge dal kv 
    return DeliveryStatus(id=d["id"], status=d["status"], drone_id=d["drone_id"])   #restituisce la risposta

@app.get("/drones/{drone_id}")                                                      #definisce l'endpoint 
async def get_drone(drone_id:str):                                                  #callback
    """
    Restituisce il documento completo di un drone.

    Args:
        drone_id (str): Identificativo del drone (es. "drone-1").

    Returns:
        dict: Documento KV del drone con campi come `status`, `pos`, `battery`, `type`, `speed`,
              `current_delivery`, `feas_miss`, `at_charge`, ecc.
    """
    return await kv_get(f"drone:{drone_id}")                                        #legge il documento del drone e lo restituisce 

@app.get("/deliveries")                                                             #definisce l'endpoint delle deliveries 
async def list_deliveries(limit: int = 30):
    """
    Elenca le delivery attive più recenti 

    Strategia:
      - Legge `deliveries_index` e considera solo una finestra finale (TAIL = max(6*limit, 180)).
      - Carica in parallelo i documenti e filtra stati attivi {pending, assigned, in_flight, delivered}.
      - Ordina per timestamp decrescente e tronca a `limit`.

    Args:
        limit (int, optional): Numero massimo di delivery da restituire. Default: 30.

    Returns:
        dict: Oggetto con chiavi:
              - `count` (int): numero di elementi in `items`.
              - `items` (List[dict]): delivery (documenti KV completi) più recenti/attive.

    """
    ids = await kv_get_opt("deliveries_index") or []                                    #legge deliveries index 
    if not ids:
        return {"count": 0, "items": []}                                                #se non c'è da una risposta vuota
                                                                                  
    TAIL = max(limit * 6, 180)                                                          #leggi una "finestra" per efficienza
    tail_ids = ids[-TAIL:]                                                              #legge solo una finestra di deliveries 

    docs = await asyncio.gather(*(kv_get_opt(f"delivery:{did}") for did in tail_ids))   #legge in paralello i documenti delle delivery nella finestra selezionata 
    docs = [d for d in docs if d]

    ACTIVE = {"pending", "assigned", "in_flight", "delivered"}
    docs = [d for d in docs if d.get("status") in ACTIVE]                               #filtra solo le richieste in stati rilevanti 

    docs.sort(key=lambda d: -float(d.get("timestamp", 0)))                              #ordina per time stamp decrescente 
    out = docs[:limit]                                                                  #taglia l'output al limit definito 
    return {"count": len(out), "items": out}                                            #restituisce la risposta 

@app.get("/drones")                                             #definisce l'endpoint 
async def list_drones():                                        #callback
    """
    Elenca i droni noti, arricchendoli con la zona corrente (se mappabile).

    Flusso:
      1) Recupera `zones_config` e `drones_index`.
      2) Legge i documenti `drone:{id}` in parallelo (bounded concurrency).
      3) Per ciascun drone determina la `zone` confrontando `pos` con i bounds di ogni cella.
      4) Restituisce l’elenco con il campo aggiuntivo `zone`.

    Args:
        Nessun parametro di ingresso.

    Returns:
        dict: Oggetto con chiavi:
              - `count` (int): numero di droni restituiti.
              - `items` (List[dict]): lista di documenti drone, ciascuno arricchito con `zone`.
    """

    zcfg = await get_zones()                                    #legge le zone chiamando get_zones
    didx = await kv_get_opt("drones_index") or []               #legge gli indici dei droni

    max_par = int(os.getenv("DRONES_FETCH_PAR", "20"))
    sem = asyncio.Semaphore(max_par)                            #imposta un limite di richieste parallele al kv 

    async def fetch(did:str):                                               #Helper che legge un drone rispettando il semaforo.
        """
        Recupera in modo asincrono il documento di un drone dal KV, rispettando un limite
        di concorrenza tramite semaforo.

        Args:
            did (str): Identificativo del drone (es. "drone-1").

        Returns:
            dict | None: Il documento KV del drone (se presente), oppure None se la chiave
            non esiste o il KV non risponde.
        """
        async with sem:
            return await kv_get_opt(f"drone:{did}")

    docs = await asyncio.gather(*(fetch(d) for d in didx))                  #Legge in parallelo chiamando fetch tutti i droni.
    docs = [d for d in docs if d]

    out = []
    for d in docs:                                                          #per ogni drone
        pos = d.get("pos") or {}                                            #prende la posizione 
        z = None
        for zz in zcfg["zones"]:                                            #vede in che zona ricade il drone 
            b = zz["bounds"]
            if b["lat_min"] <= pos.get("lat", 999) <= b["lat_max"] and \
               b["lon_min"] <= pos.get("lon", 999) <= b["lon_max"]:
                z = zz["name"]; break
        d["zone"] = z
        out.append(d)
    return {"count": len(out), "items": out}                                #restituisce la risposta 
