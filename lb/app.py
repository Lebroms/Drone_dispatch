import os, time, json, asyncio
from fastapi import FastAPI, Request, Response  #request per leggere la richiesta del client, response per costruire la risposta da invairgli
import httpx
import socket
from urllib.parse import urlparse
from httpx import Limits

# Cache IP backend + indice RR
_backend_ips: list[tuple[str, int]] = []      #lista degli IP (host, porta) risolti dal DNS per il nome gateway  (usiamo il dns solo per scoprire gli ip dei container)
_rr_idx = 0                                   #contatore che gira sui backend per scegliere chi tocca (round robin).
_rr_lock = asyncio.Lock()                     #lock asincrono per garantire che due richieste simultanee non incrementino _rr_idx insieme.
_last_resolve = 0.0

RESOLVE_TTL_SEC = float(os.getenv("LB_RESOLVE_TTL_SEC", "5"))    # ogni quanti secondi rifare la DNS
TARGET_URL = os.getenv("TARGET_URL", "http://gateway:8000")      # url di destinazione 

# ==== Rate Limiter config (env) ====
RL_GLOBAL_RATE  = float(os.getenv("RL_GLOBAL_RATE",  "0"))       # token/sec; 0 = OFF
RL_GLOBAL_BURST = float(os.getenv("RL_GLOBAL_BURST", "0"))

def _parse_target(url: str) -> tuple[str, int, bool]:
    """
    Analizza un URL e ne estrae host, porta e tipo di schema.

    Questa funzione usa `urllib.parse.urlparse` per scomporre l'URL in
    componenti e restituisce una tupla con:
      - hostname (str): l'host dell'URL; se mancante viene usato "gateway".
      - port (int): la porta dell'URL; se mancante viene usata 80 (default HTTP).
      - is_http (bool): True se lo schema è "http", False altrimenti (es. "https").

    Args:
        url (str): L'URL di destinazione

    Returns:
        tuple[str, int, bool]: Una tupla contenente:
            - hostname (str): l'host da risolvere.
            - port (int): la porta numerica.
            - is_http (bool): indicatore se lo schema è HTTP.

    """
    u = urlparse(url)            #spezza l'url nei suoi componenti
    # httpx/uvicorn parlano HTTP, quindi True per http
    return (u.hostname or "gateway", u.port or 80, u.scheme == "http")     #crea la tupla con i vari pezzi dell'url


app = FastAPI(title="LB)")  #crea l'app fastapi

# ===== access log per SLO passivo =====
'''@app.middleware("http")
async def _access_log(request: Request, call_next):
    t0 = time.perf_counter()
    status_code = None
    try:
        resp = await call_next(request)
        status_code = resp.status_code
        return resp
    finally:
        dt_ms = (time.perf_counter() - t0) * 1000.0
        path = request.url.path or "/"
        if path.startswith("/deliveries"):
            if status_code is None:
                status_code = 500
            print(f"[access] {request.method} {path} {status_code} {dt_ms:.1f}ms")'''

# ==== Stato Token Bucket (globale) (time.monotonic:restituisce un contatore crescente da quando il sistema viene avviato)====
_global_bucket = {"tokens": RL_GLOBAL_BURST, "last": time.monotonic()}        #inizializza il bucket globale, last:memorizza il timestamp monotonic dell’ultima volta che abbiamo fatto refill.
                                                                              #questo perchè il refill avviene solo quando arrivano nuove richieste 
                                                                              #in base al tempo tra l'ultimo refill e la nuova richiesta ricarichiamo


def _refill(bucket: dict, rate: float, burst: float) -> None:  
    """
    Ricarica i token di un bucket in base al tempo trascorso dall'ultimo refill.

    Args:
        bucket (dict): Il dizionario che rappresenta il bucket, con chiavi:
            - "tokens" (float): Numero attuale di token disponibili.
            - "last" (float): Timestamp monotonic dell'ultimo refill.
        rate (float): Velocità di ricarica, in token al secondo.
        burst (float): Capacità massima del bucket (limite superiore).

    Returns:
        None: La funzione aggiorna direttamente il contenuto del bucket.

    """
    now = time.monotonic()
    elapsed = now - bucket["last"]                                       #legge l'ultima volta che è stato fatto il refill dal bucket in ingresso 
    bucket["tokens"] = min(burst, bucket["tokens"] + elapsed * rate)     #aggiunge token senza superare il limite 
    bucket["last"] = now                                                 #aggiorna il tempo dell'ultimo refill

def _take(bucket: dict, cost: float = 1.0) -> bool:
    """
    Prova a consumare un certo numero di token da un bucket.

    Args:
        bucket (dict): Il dizionario del bucket con chiave "tokens" (float).
        cost (float, optional): Numero di token richiesti per la richiesta.
            Default = 1.0.

    Returns:
        bool: True se il bucket aveva abbastanza token (e li ha scalati),
              False se non c'erano token sufficienti.

    """
    if bucket["tokens"] >= cost:                  #se ha un numero sufficiente di token li scala
        bucket["tokens"] -= cost
        return True
    return False

def _retry_after(tokens: float, rate: float, cost: float = 1.0) -> int:
    """
    Calcola il tempo minimo di attesa (Retry-After) in secondi prima che
    il bucket abbia abbastanza token per soddisfare la richiesta.

    Args:
        tokens (float): Numero di token attualmente disponibili.
        rate (float): Velocità di ricarica, in token al secondo.
        cost (float, optional): Numero di token richiesti per la richiesta.
            Default = 1.0.

    Returns:
        int: Numero di secondi da attendere (almeno 1).
    """
    if rate <= 0: return 1
    deficit = max(0.0, cost - tokens)                           #Calcola quanti token mancano per poter servire la richiesta.
    return max(1, int(deficit / rate))                          #deficit/rate:calcola quanti secondi teorici servono prima di avere abbastanza token.



async def _resolve_backend_ips(host: str, port: int) -> list[tuple[str, int]]:
    """
    Risolve un nome host in una lista di indirizzi IP utilizzabili per la connessione TCP.

    Questa funzione interroga il DNS in maniera asincrona (via getaddrinfo) e
    restituisce tutti gli indirizzi IP validi per l'host e la porta specificati,
    eliminando eventuali duplicati ma preservando l'ordine originale.

    Args:
        host (str): Nome host da risolvere (es. "gateway").
        port (int): Porta TCP del servizio (es. 8000).

    Returns:
        list[tuple[str, int]]: Lista di coppie (ip, port), una per ogni indirizzo
        IP valido risolto per l'host.
        - ip (str): Indirizzo IP del backend.
        - port (int): Porta associata al backend.
    """
    loop = asyncio.get_running_loop()
    infos = await loop.getaddrinfo(host, port, type=socket.SOCK_STREAM)            #interroga il DNS chiedendo di risolvere l'host dando tutti i risultati
    # deduplica preservando ordine
    out, seen = [], set()                                                          #lista finale con gli indirizzi IP senza duplicati.
    for ai in infos:                                                               #Itera su ciascun record DNS risolto.
        ip, prt = ai[4][0], ai[4][1]                                               #con 4 estraiamo l'ultimo elemento della tupla presente in infos che contiene una tupla (ip, port) che viene estratta 
        if (ip, prt) not in seen:                                                  #aggiunge la coppia se non l'ha vista 
            seen.add((ip, prt))
            out.append((ip, prt))
    return out

async def _pick_backend_base() -> str:

    """
    Seleziona un backend a cui inoltrare la prossima richiesta, usando round-robin.

    La funzione:
      1. Analizza TARGET_URL per ottenere host e porta.
      2. Se la cache degli IP (_backend_ips) è vuota o scaduta, invoca
         _resolve_backend_ips per rinfrescare la lista.
      3. Se non ci sono IP validi, effettua fallback restituendo TARGET_URL.
      4. Altrimenti, sceglie un IP dalla lista con politica round-robin
         (_rr_idx % len(_backend_ips)), incrementa l’indice, e costruisce
         l’URL finale.

    Returns:
        str: Base URL del backend scelto, nel formato "http://<ip>:<port>".

    """
    global _backend_ips, _rr_idx, _last_resolve                 #usa le variabili globali
    host, port, is_http = _parse_target(TARGET_URL)             #Chiama _parse_target che restituisce host port e se ha lo schema http


    # refresh lista IP ogni RESOLVE_TTL_SEC
    now = time.monotonic()                                                      #inizializza il timer crescente
    if not _backend_ips or (now - _last_resolve) > RESOLVE_TTL_SEC:             #controlla se backend_ips è vuota o se è scaduto il tempo dopo cui deve richiedere di nuovo
        _backend_ips = await _resolve_backend_ips(host, port)                   #che chiede al DNS tutti gli IP registrati per quell'host
        _last_resolve = now                                                     #aggiorna il tempo dell'ultima risoluzione DNS 

    if not _backend_ips:                                              #Se dopo la risoluzione non ci sono IP validi, usa l’URL originale
        
        return TARGET_URL.rstrip("/")

    async with _rr_lock:
        ip, prt = _backend_ips[_rr_idx % len(_backend_ips)]           #in base al numero della richiesta fa la divisione intera e dice a che backend inviare 
        _rr_idx += 1
    scheme = "http"                                                   #per il tuo stack
    return f"{scheme}://{ip}:{prt}"                                   #Costruisce l' URL del backend scelto. Viene poi usata dal proxy per inoltrare la richiesta.





# ====== HTTP client riusabile (KEEP-ALIVE) ======
@app.on_event("startup")
async def _startup(): 
    """
    Evento di avvio dell'app FastAPI.

    Crea un client HTTP asincrono globale (httpx.AsyncClient) da riutilizzare
    per tutte le richieste proxate al backend
    """
    app.state.http = httpx.AsyncClient(timeout=5.0,limits=Limits(max_connections=200, max_keepalive_connections=10))  # Crea un client HTTP asincrono e lo mette in app.state

@app.on_event("shutdown")
async def _shutdown(): 
    """
    Evento di spegnimento dell'app FastAPI.

    Chiude in maniera ordinata il client HTTP globale creato in fase di startup,
    liberando risorse di rete e connessioni pendenti.    
    """
    await app.state.http.aclose() #Chiude il client

@app.get("/health")
async def health():
    """
    Endpoint di health-check per verificare che il servizio sia attivo.

    Verifica rapidamente che il load balancer sia attivo e risponda,
    senza coinvolgere né il backend né RabbitMQ né il KV.

    Returns:
        dict: JSON con un singolo campo:
            - "status" (str): sempre "ok" se l'app è viva.
    """
    return {"status": "ok"}

@app.api_route("/{path:path}", methods=["GET","POST","PUT","DELETE","PATCH","OPTIONS","HEAD"])     #definisce un endpoint che riceve qualasisi tipo di richiesta dal client 
async def proxy(request: Request, path: str):
    """
    Funzione proxy principale del Load Balancer.

    Riceve una richiesta HTTP in ingresso, applica logica di rate limiting,
    sceglie un backend secondo round-robin DNS e inoltra la richiesta al
    backend selezionato tramite httpx.AsyncClient.
    Comportamento:
        1. Costruisce l'URL target concatenando il backend scelto + path + query string.
        2. Copia gli headers, rimuove `Host` e aggiunge `X-Forwarded-For` con l'IP del client.
        3. Legge il corpo (`body`) e normalizza il metodo (`method.upper()`).
        4. Applica rate limiter globale (esclusi `/health` e `/zones`).
        5. Inoltra la richiesta al backend (con retry solo per metodi safe o POST idempotenti).
        6. Restituisce al client la stessa risposta ricevuta dal backend (content, status, headers).

    Args:
        request (Request): Oggetto FastAPI che incapsula la richiesta HTTP
            originale del client (path, query, headers, body, ecc.).
        path (str): Porzione di path dinamico catturata dall'URL. Ad esempio,
            una richiesta a `/deliveries/123` arriva qui come `path="deliveries/123"`.

    Returns:
        Response: La risposta HTTP finale verso il client, costruita a partire
        dalla `httpx.Response` del backend (propaga content, status code e headers).

    """
    qs = request.url.query                                 #Estrae la query string
    base = await _pick_backend_base()
    url = f"{base}/{path}" + (f"?{qs}" if qs else "")      #costruisce l'url, ogni richiesta viene indirizzata a un IP diverso, in round robin.
    

    # copia headers (senza 'host') + x-forwarded-for
    headers = {k: v for k, v in request.headers.items() if k.lower() != "host"}         #copiamo l'header togliendo l'host cosi poi lo reimposta il backend
    headers["x-forwarded-for"] = request.client.host
    body = await request.body()                                                         #legge il corpo della richiesta 
    method = request.method.upper()                                                     #normalizza il metodo in maiuscolo

    # ==== RATE LIMITER globale (escludi health/zones) ====
    path_full = request.url.path or "/"
    if path_full not in ("/health", "/zones"):                                          #non applica rate limit a /health e /zones
        if RL_GLOBAL_RATE > 0 and RL_GLOBAL_BURST > 0:
            _refill(_global_bucket, RL_GLOBAL_RATE, RL_GLOBAL_BURST)                    #fa il refill
            if not _take(_global_bucket, 1.0):                                          #prova a prendere un token e se fallisce calcola dopo quanto riprovarci e risponde 429
                ra = _retry_after(_global_bucket["tokens"], RL_GLOBAL_RATE, 1.0)
                return Response(
                    content=json.dumps({"detail": "rate limit (global)"}),
                    status_code=429,
                    headers={"Retry-After": str(ra), "Content-Type": "application/json"}
                )

    # Consente un solo retry solo per: metodi safe/idempotenti: GET/HEAD/PUT/DELETE, oppure POST con Idempotency-Key (idempotenza applicativa lato backend).
    allow_retry = (method in {"GET","HEAD","PUT","DELETE"}) or \
                  (method == "POST" and request.headers.get("idempotency-key") is not None)

    async def one_try():
        """
        Inoltra la richiesta corrente al backend target utilizzando il client HTTP
        condiviso (`httpx.AsyncClient`).

        Args:
            Nessuno. I parametri necessari (metodo, URL, headers, body) sono catturati
            dalla closure esterna del proxy:
                - method (str): Metodo HTTP della richiesta originaria (es. "GET", "POST").
                - url (str): URL completo verso il backend (`TARGET_URL` + path + query).
                - headers (dict): Intestazioni HTTP da propagare al backend, con
                "Host" rimosso e "X-Forwarded-For" aggiunto.
                - body (bytes): Corpo raw della richiesta (es. JSON, form data).

        Returns:
            httpx.Response: La risposta restituita dal backend, comprensiva di
            contenuto, status code e headers.
        """
        return await app.state.http.request(method, url, headers=headers, content=body)

    try:
        resp = await one_try()
    except Exception:
        if not allow_retry:
            raise
        resp = await one_try()  # un solo retry

    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers=dict(resp.headers),
        media_type=resp.headers.get("content-type")
    )
