import os
import time
import json
import sqlite3
import threading
from collections import OrderedDict
from typing import Any, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="KV Store (PoC)")

# ======================
# Config
# ======================
DB_FILE = os.getenv("DB_FILE", "/data/kv_store.db") #/data/kv_store.db è il percorso del file SQLite dentro al container Docker che esegue il servizio kvstore.
MAX_CACHE_ITEMS = int(os.getenv("MAX_CACHE_ITEMS", "10000")) #massimo numero di elementi nella cache in RAM.
MAX_CACHE_SIZE_BYTES = int(os.getenv("MAX_CACHE_SIZE_BYTES", "33554432"))  # limite in byte della cache

# ======================
# Cache LRU thread-safe (chiave -> oggetto Python)
# ======================
class LRUCache:
    """
    Implementazione thread-safe di una cache LRU (Least Recently Used).

    La cache mantiene al massimo un certo numero di elementi (`max_items`) 
    o una dimensione complessiva massima (`max_bytes`). Quando si supera 
    uno di questi limiti, gli elementi meno usati di recente vengono 
    automaticamente rimossi.

    Attributes:
        max_items (int): Limite di elementi nella cache.
        max_bytes (int): Limite di memoria in byte.
        _lock (threading.RLock): Lock per garantire sicurezza thread-safe.
        _d (OrderedDict[str, Any]): Struttura che memorizza chiavi/valori
            mantenendo l'ordine di utilizzo.
        _size_bytes (int): Dimensione attuale (in byte) della cache.
    """
    def __init__(self, max_items: int, max_bytes: int):
        """
        Costruttore della classe. Setta gli attributi inserendo i limiti della cache e istanziando le strutture dati per gestirla

        Args:
            max_items (int): Numero massimo di elementi nella cache.
            max_bytes (int): Dimensione massima totale (in byte) della cache.
        """
        self.max_items = max_items
        self.max_bytes = max_bytes
        self._lock = threading.RLock()
        self._d: "OrderedDict[str, Any]" = OrderedDict() #OrderedDict in Python mantiene l’ordine di inserimento delle chiavi. Rappresneta la cache
        self._size_bytes = 0

    def _sizeof(self, k: str, v: Any) -> int:
        """
        Stima la dimensione, in byte, dell'entry (k, v) in cache.

        Args:
            k (str): Chiave dell'entry.
            v (Any): Valore dell'entry (idealmente JSON-serializzabile).

        Returns:
            int: Stima della dimensione complessiva in byte.
        """
        try:
            #prova a stimare accuratamente
            #len(k.encode("utf-8")) converte la chiave in una sequenza di byte UTF-8 e conta i byte di quella sequenza
            #len(json.dumps(v, separators=(",", ":")).encode("utf-8")) trasforma il valore v in formato json, converte la stringa JSON in una sequenza di byte UTF-8.
            #Conta quanti byte occupa quella rappresentazione JSON.
            return len(k.encode("utf-8")) + len(json.dumps(v, separators=(",", ":")).encode("utf-8"))
        except Exception:#se fallisce fa una stima approssimativa(la dimensione è la lunghezza della chiave più una quota fissa di 32 byte per il valore.)
            return len(k.encode("utf-8")) + 32

    def get(self, k: str) -> Optional[Any]:
        """
        Recupera un valore dalla cache (se presente) e aggiorna l'ordine LRU.

        Args:
            k (str): Chiave da cercare nella cache.

        Returns:
            Optional[Any]: 
                - Il valore associato se la chiave è in cache.
                - None se la chiave non esiste.
        """
        with self._lock: ## acquisisce un lock per rendere thread-safe l’accesso/modifica alla cache
            if k not in self._d: #se la chiave non è nel dizionario interno restituisce none 
                return None
            v = self._d.pop(k)  #rimuove la chiave k dalla posizione attuale nella cache, restituendo il valore associato.
            
            self._d[k] = v #reinserisce la chiave k in fondo all’OrderedDict. (quella usata più recentemente)
            return v

    def put(self, k: str, v: Any) -> None:
        """
        Inserisce o aggiorna una chiave nella cache (write-through).

        Args:
            k (str): Chiave da inserire o aggiornare.
            v (Any): Valore associato alla chiave.
        """
        with self._lock: #garantisce che solo un thread alla volta modifichi la cache.
            if k in self._d: #Controlla se la chiave k è già presente, se si va aggiornata
                old = self._d.pop(k) #rimuove il valore precedente
                self._size_bytes -= self._sizeof(k, old) #aggiorna la dimensione della cache sottraendo il peso della coppia chiave valore con il valore vecchio
            self._d[k] = v#Inserisce la nuova coppia (è la più recente)
            self._size_bytes += self._sizeof(k, v) #aggiorna la dimensione della cache aggiungendo il peso della coppia chiave valore con il valore vecchio
            self._evict() #dopo l'aggiornamento chiama evict 

    def delete(self, k: str) -> None:
        """
        Elimina una chiave dalla cache, se presente.

        Args:
            k (str): Chiave da eliminare dalla cache.
        """
        with self._lock: ##garantisce che solo un thread alla volta modifichi la cache.
            if k in self._d:#Controlla se la chiave k è già presente
                old = self._d.pop(k) ##rimuove la coppia chiave valore presente
                self._size_bytes -= self._sizeof(k, old) #aggiorna la dimensione della cache sottraendo il peso della coppia chiave valore

    def clear(self) -> None:
        """
        Funzione che svuota completamente la cache
        """

        
        with self._lock:#garantisce che solo un thread alla volta modifichi la cache.
            self._d.clear() #svupta OrderedDict
            self._size_bytes = 0 #resetta il contatore della dimensione totale

    def _evict(self) -> None:
        """
        Funzione che mantiene la cache entro i limiti di capacità.

        """
        # Evict by size then by items
        while self._size_bytes > self.max_bytes and self._d: #Se la cache occupa più byte del massimo consentito e non è vuota…
            k, v = self._d.popitem(last=False) #rimuove il primo elemento inserito (il più vecchio cioè quello meno usato recentemente)
            self._size_bytes -= self._sizeof(k, v) #aggiorna la dimensione della cache sottraendo la stima della coppia appena rimossa
        while len(self._d) > self.max_items and self._d: #Se il numero di item è maggiore del massimo consentito… compie le stesse operazioni
            k, v = self._d.popitem(last=False)
            self._size_bytes -= self._sizeof(k, v)

CACHE = LRUCache(MAX_CACHE_ITEMS, MAX_CACHE_SIZE_BYTES) #istanzia un oggetto di classe LRUCache

# ======================
# DB helpers (SQLite)
# ======================
_db_lock = threading.RLock()  #variabile per gestire gli accessi multipli al db
_conn: Optional[sqlite3.Connection] = None #conterrà l’oggetto connessione SQLite

def _connect_db() -> sqlite3.Connection:
    """
    Crea e ritorna una connessione SQLite configurata per il KV store.

    - Usa `DB_FILE` come percorso del database (creato dal caller allo startup).
    - `check_same_thread=False` per permettere l'uso della connessione da thread diversi
      (proteggeremo le operazioni con `_db_lock`).
    - `isolation_level=None` abilita la modalità autocommit; le transazioni esplicite
      verranno aperte a mano (es. `BEGIN IMMEDIATE` in `db_cas`).

    Returns:
        sqlite3.Connection: connessione aperta e pronta all'uso.
    """
    conn = sqlite3.connect(DB_FILE, check_same_thread=False, isolation_level=None) #apre la connessione che puòessere usata da più thread contemporaneamente
    #aggiungono funzionalità alla connessione sqlite
    conn.execute("PRAGMA journal_mode=WAL;") #Scrive prima le modifiche in un file WAL separato, poi le applica al DB in blocco.
    conn.execute("PRAGMA synchronous=NORMAL;")#quanto duramente SQLite assicura che i dati siano davvero scritti su disco.
    conn.execute("PRAGMA temp_store=MEMORY;") #Decide dove mettere le tabelle temporanee per query complesse
    conn.execute("PRAGMA foreign_keys=ON;")#applica i vincoli di foreign_key
    return conn #restituisce la connessione

def _init_db(conn: sqlite3.Connection) -> None:
    """
    Inizializza lo schema del database KV se non esiste già.

    Crea la tabella `kv_store` per salvare le coppie chiave–valore e 
    un indice opzionale sulla colonna `updated_at`.

    Struttura tabella:
        - key (TEXT, PRIMARY KEY): 
            Identificatore univoco della voce KV.
        - value (TEXT, NOT NULL): 
            Valore serializzato in formato JSON.
        - updated_at (REAL, NOT NULL): 
            Timestamp UNIX (float) dell’ultima modifica, 
            usato per politiche di last-write-wins e per audit.

    Args:
        conn (sqlite3.Connection): Connessione attiva al database SQLite.

    Returns:
        None. La funzione modifica lo schema del database se necessario.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS kv_store (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at REAL NOT NULL
        );
    """)#Esegue un comando SQL sul DB collegato a conn, key colonna di tipo testo, value colonna tipo testo not null dove viene salvato il valore associato alla chiave 
    #updated_at colonna di tipo real not null dove c'è timestamp dell'ultima modifica
    # Crea un indice secondario sulla colonna updated_at opzionale per eventuali ispezioni
    conn.execute("CREATE INDEX IF NOT EXISTS idx_kv_updated ON kv_store(updated_at);")

@app.on_event("startup")#Registra la funzione come handler dell’evento di startup
def _startup():
    """
    Evento di avvio dell'applicazione.
    """
    global _conn #dichiarazione di uso della variabile globale
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True) #Crea la directory che contiene il file del DB
    _conn = _connect_db() #Apre la connessione al DB e applica i PRAGMA assegnandola alla variabile globale
    _init_db(_conn) #crea la tabella kv_store e l’indice se mancano (operazione idempotente)

@app.on_event("shutdown")
def _shutdown():
    """
    Evento di spegnimento dell'applicazione.
    """
    global _conn #dichiarazione di uso della variabile globale
    if _conn is not None:
        _conn.close() #Chiude in modo pulito la connessione SQLite con il metodo di default della classe sqlite.connection
        _conn = None #resetta la variabile locale

def db_get(key: str) -> Optional[Any]:
    """
    Recupera un valore dal DB SQLite per una chiave.

    Args:
        key (str): La chiave da cercare.

    Returns:
        Any | None:
            - Il valore deserializzato da JSON se trovato.
            - `None` se la chiave non esiste.
            - In fallback: la stringa grezza se non è un JSON valido.
    """
    with _db_lock: #Serve a fare in modo che solo un thread per volta entri nella sezione 
        cur = _conn.execute("SELECT value FROM kv_store WHERE key = ?;", (key,)) #Esegue la query parametrizzata per selezionare la colonna value associata a quella key.
        # cur è un oggetto che rappresenta il puntatore al risultato della query.
        row = cur.fetchone() #estrae il valore associato alla chiave
    if not row:
        return None
    try:
        return json.loads(row[0]) #ricostruiamo l'oggetto originale python che rappresnetava quella stringa json 
    except Exception:
        # Fallback: se per qualche motivo non è JSON
        return row[0]

def db_put(key: str, value: Any) -> None:
    """
    Inserisce o aggiorna un valore nel DB SQLite per la chiave data.

    Args:
        key (str): La chiave da scrivere.
        value (Any): Il valore da salvare. Verrà serializzato in JSON.
    """
    val_json = json.dumps(value, separators=(",", ":")) #trasforma value in una stringa json eliminando gli spazi
    ts = time.time() #Prende il timestamp corrente in secondi
    with _db_lock: #Serve a fare in modo che solo un thread per volta entri nella sezione 
        _conn.execute(
            "INSERT INTO kv_store(key, value, updated_at) VALUES(?,?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at;",
            (key, val_json, ts)
        )#Esegue una query tale per cui se la chiave è nuova fa INSERT se la chiave esiste fa UPDATE del valore e del ts

def db_cas(key: str, old: Any, new: Any) -> bool:
    """
    Esegue una Compare-And-Swap (CAS) atomica sulla chiave indicata.

    La funzione aggiorna il valore solo se quello corrente nel DB corrisponde
    a `old`. In questo modo si evitano race condition e update persi.

    Args:
        key (str): La chiave su cui fare la CAS.
        old (Any): Valore atteso attuale (deserializzato). 
                   Se `None`, ci si aspetta che la chiave non esista.
        new (Any): Nuovo valore da scrivere in caso di successo.

    Returns:
        bool:
            - `True` se la CAS è andata a buon fine (update/insert eseguito).
            - `False` se la condizione non era soddisfatta (valore diverso o chiave già presente).
    """
    ts = time.time() #Timestamp attuale
    new_json = json.dumps(new, separators=(",", ":"))#Serializza il nuovo valore in JSON compatto
    with _db_lock:#Serve a fare in modo che solo un thread per volta entri nella sezione 
        _conn.execute("BEGIN IMMEDIATE;")
        cur = _conn.execute("SELECT value FROM kv_store WHERE key = ?;", (key,)) #Legge il valore corrente associato alla chiave.
        row = cur.fetchone() #estrae il contenuto del campo value

        if row is None: #se assente perchè la chiave non c'è
            # chiave assente
            if old is None:
                _conn.execute(
                    "INSERT INTO kv_store(key, value, updated_at) VALUES(?,?,?);",
                    (key, new_json, ts)
                )#inseriamo new come nuovo valore perchè prima era vuoto il valore
                _conn.execute("COMMIT;")
                return True
            _conn.execute("ROLLBACK;") #altrimenti fallisce
            return False

        # chiave presente: confronto tra 'old' e 'current'
        try:
            current_obj = json.loads(row[0]) #tenta di riconvertire in oggetto python il json salvato
        except Exception:
            # se in DB non è JSON, il confronto con un dict/list non potrà mai riuscire
            current_obj = row[0]

        if old is None: #il chiamante si aspetta che sia none ma siamo nel ramo che la chiave esiste quindi fallisce
        
            _conn.execute("ROLLBACK;")
            return False

        if current_obj == old: #il chiamante vuole che venga aggiornato solo se il valore attuale è uguale a quello passato
            _conn.execute(
                "UPDATE kv_store SET value=?, updated_at=? WHERE key=?;",
                (new_json, ts, key)
            )#fa l'update mettendo il nuovo valore e il nuovo ts
            _conn.execute("COMMIT;")
            return True

        _conn.execute("ROLLBACK;")
        return False


# ======================
# Locks (in-process, invariati)
# ======================
STORE_LOCKS: dict[str, float] = {}  # key -> expiry_ts #un dizionario in memoria locale (non nel DB). serve per implementare un sistema di lock distribuiti

class ValueModel(BaseModel):
    """
    Modello Pydantic per la richiesta PUT su /kv/{key}.

    Attributes:
        value (Any): Valore associato alla chiave KV.
                     Può essere dict, list, stringa, numero, ecc.
    """
    value: Any

class CasModel(BaseModel):
    """
    Modello Pydantic per la richiesta POST su /kv/cas.

    Attributes:
        key (str): La chiave su cui eseguire la CAS.
        old (Any): Valore atteso attuale; se non coincide la CAS fallisce.
        new (Any): Nuovo valore da scrivere se 'old' coincide.
    """
    key: str
    old: Any
    new: Any

# ======================
# API
# ======================
@app.get("/health")
def health():
    """
    Endpoint di health check.

    Returns:
        dict: Dizionario JSON con lo stato del servizio.
              
    """
    return {"status": "ok"}

@app.get("/kv/{key}")
def get_key(key: str):
    """
    Recupera il valore associato a una chiave dal KV store.

    Args:
        key (str): Nome della chiave da cercare.

    Returns:
        dict: JSON contenente:
            - "key": la chiave richiesta.
            - "value": il valore associato alla chiave.

    """
    
    v = CACHE.get(key) #cerca la chiave nella cache
    if v is not None: #se esiste la restituisce
        return {"key": key, "value": v}
    
    v = db_get(key) #la chiede al db 
    if v is None:
        raise HTTPException(status_code=404, detail="Key not found")
    
    CACHE.put(key, v) #aggiorna la cache in modo che questo sia l'ultimo valore usato
    return {"key": key, "value": v}

@app.put("/kv/{key}")
def put_key(key: str, body: ValueModel):
    """
    Scrive o aggiorna un valore nel KV store.

    Args:
        key (str): Chiave da inserire o aggiornare.
        body (ValueModel): Oggetto contenente il campo "value" con il dato da salvare.

    Returns:
        dict: JSON {"ok": True} se l’operazione è andata a buon fine.

    """
    # write-through: DB poi cache
    db_put(key, body.value) #scrive sul db
    CACHE.put(key, body.value) #scrive in cache (come ultimo elemento appena visto)
    return {"ok": True}

@app.post("/kv/cas")
def cas(body: CasModel):
    """
    Esegue una operazione CAS (Compare-And-Swap) atomica sul KV store locale.

    Args:
        body (CasModel): Oggetto con:
            - key (str): Chiave da aggiornare.
            - old (Any): Valore atteso attuale.
            - new (Any): Nuovo valore da scrivere se `old` corrisponde.

    Returns:
        dict:
            - {"ok": True} se l’aggiornamento è avvenuto.
            - {"ok": False, "current": <valore_attuale>} se fallisce
              (il valore corrente non corrisponde a `old`).
    """
    ok = db_cas(body.key, body.old, body.new)  #fa il cas nel db chiamando l'helper
    if ok:
        
        CACHE.put(body.key, body.new) # aggiorna cache con il nuovo valore
        return {"ok": True}
    #se fallisce il cas ritorna il valore attuale 
    cur = db_get(body.key)
    return {"ok": False, "current": cur}

@app.post("/lock/acquire/{key}")
def lock_acquire(key: str, ttl_sec: int = 30):
    """
    Acquisisce un lock locale sul KV store.

    Args:
        key (str): Nome del lock.
        ttl_sec (int, default=30): Durata in secondi del lock (time-to-live).

    Returns:
        dict:
            - {"ok": True} se il lock è stato acquisito.
            - {"ok": False, "expires_at": <timestamp>} se il lock è già occupato.
    """
    now = time.time() ##Timestamp attuale
    exp = STORE_LOCKS.get(key, 0.0) #Legge dalla mappa STORE_LOCKS la scadenza corrente del lock key (se non esiste usa 0)
    if now >= exp:#Se adesso è oltre (o uguale) la scadenza salvata, il lock è libero o scaduto.
        STORE_LOCKS[key] = now + ttl_sec #imposta il lock scrivendo una nuova scadenza
        return {"ok": True}
    return {"ok": False, "expires_at": exp} #se non è oltre restituisce tra quando scade il lock attivo

@app.post("/lock/release/{key}")
def lock_release(key: str):
    """
    Rilascia un lock locale precedentemente acquisito.

    Args:
        key (str): Nome del lock da liberare.

    Returns:
        dict: {"ok": True}, anche se il lock non era presente.
        
    """
    STORE_LOCKS.pop(key, None) #Rimuove la chiave del lock dalla mappa STORE_LOCKS (operazione idempotente, se non esiste non fa nulla)
    return {"ok": True}
