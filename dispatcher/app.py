#  orchestrazione event-driven + loop periodico 
# - Consuma delivery_requests  -> assign_one(...)
# - Consuma drone_updates      -> advance_for_drone(...) mirato
# - Produce delivery_status    -> assigned / completed
# - Loop periodico             -> autoscale, charging/retiring, advance_deliveries, assign_round
# - Consistenza: lock best-effort + CAS *obbligatoria* nelle transizioni critiche

import os, json, math, asyncio
import httpx
import aio_pika

# ====== Config ======
RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq/")            #Legge la variabile d’ambiente RABBIT_URL; se assente, default all’URL AMQP del broker RabbitMQ interno al compose.
KV_URL     = os.getenv("KV_URL", "http://kvfront:9000")                         #URL base del servizio kvfront

DELIVERY_REQ_QUEUE    = os.getenv("DELIVERY_REQ_QUEUE", "delivery_requests")    #coda AMQP da cui il dispatcher consuma richieste di consegna.
DRONE_UPDATES_QUEUE   = os.getenv("DRONE_UPDATES_QUEUE", "drone_updates")       #coda AMQP dove il drone_sim pubblica aggiornamenti dei droni; il dispatcher li consuma per avanzare lo stato.
DELIVERY_STATUS_QUEUE = os.getenv("DELIVERY_STATUS_QUEUE", "delivery_status")   #coda AMQP su cui il dispatcher pubblica eventi di stato delle delivery

ASSIGNER_TICK_MS        = int(os.getenv("ASSIGNER_TICK_MS", "200"))             #Periodo (in millisecondi) dello scheduler periodico
PENDING_SCAN_LIMIT      = int(os.getenv("PENDING_SCAN_LIMIT", "500"))           #Quante delivery pending leggere per round 
MAX_ASSIGN_PER_ROUND    = int(os.getenv("MAX_ASSIGN_PER_ROUND", "100"))         #Limite massimo di assegnazioni che assign_round può tentare per tick

BATTERY_PER_KM          = float(os.getenv("BATTERY_PER_KM", "2"))               #Consumo batteria (in punti percentuali) per km stimato
SAFETY_MARGIN_PCT       = float(os.getenv("SAFETY_MARGIN_PCT", "5.0"))          #Margine di sicurezza percentuale aggiunto al fabbisogno energetico della missione
NEAR_EPS_KM             = float(os.getenv("NEAR_EPS_KM", "0.2"))                #Granularità per “bucket” di distanza nel ranking dei candidati
MAX_PICKUP_KM           = float(os.getenv("MAX_PICKUP_KM", "20.0"))             #Distanza massima consentita tra drone e origin per poter prendere l’ordine.
ARRIVE_EPS_KM           = float(os.getenv("ARRIVE_EPS_KM", "0.02"))             #Raggio (in km) per considerare “arrivato” a origin/destination e cambiare leg/stato

CRITICAL_BATTERY        = float(os.getenv("CRITICAL_BATTERY", "30.0"))          #Soglia percentuale sotto la quale il drone va mandato in charging
FULL_AFTER              = float(os.getenv("FULL_AFTER", "95.0"))                #Percentuale batteria considerata “quasi piena”; usata per transizioni di stato
EARLY_CHARGE_THRESHOLD  = int(os.getenv("EARLY_CHARGE_THRESHOLD", "5"))         #Dopo quanti fallimenti di fattibilità (feas_miss) si forza il drone in charging.

DRONE_POOL_MAX          = int(os.getenv("DRONE_POOL_MAX", "20"))                #Limite superiore dimensionale della flotta gestita dal drone_sim
BASE_ACTIVE             = int(os.getenv("BASE_ACTIVE", "4"))                    #Minimo di droni “attivi” (per classe distribuiti) quando non c’è backlog;
SCALE_RATIO             = float(os.getenv("SCALE_RATIO", "0.8"))                #Rapporto backlog→droni target; scala il numero di attivi in funzione delle pending.

SCHED_LOCK = asyncio.Lock()                                                     #mutex asincrono, serve per non fare accavallare due coroutine
DEBUG_CHARGE = os.getenv("DEBUG_CHARGE", "1") == "1"                            #accende/spegne i log legati alla carica.

def _log_charge(reason: str, did: str, **kw):
    """
    Log helper per eventi legati a battery/charging.

    Args:
        reason: Etichetta del motivo/contesto di log (es. 'pick_drone:battery_critical').
        did: ID del drone.
        **kw: Coppie chiave=valore aggiuntive (batt, crit, miss, thr, ctx, ...).

    Returns:
        None
    """
    if not DEBUG_CHARGE:
        return
    kv = " ".join(f"{k}={kw[k]}" for k in kw)
    print(f"[charge][{reason}] {did} {kv}")                                 # esempio: [charge][pick_drone:battery_critical] drone-7 batt=28.0 crit=30.0 miss=2 thr=5 ctx=pre-pick


# ======== util ========
def haversine_km(a, b):
    """
    Calcola la distanza in km (Haversine) tra due punti geografici.

    Args:
        a: Dict con chiavi 'lat' e 'lon' (gradi).
        b: Dict con chiavi 'lat' e 'lon' (gradi).

    Returns:
        float: Distanza in km tra a e b.
    """
    R = 6371.0
    import math as _m
    lat1, lon1 = _m.radians(a["lat"]), _m.radians(a["lon"])
    lat2, lon2 = _m.radians(b["lat"]), _m.radians(b["lon"])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    h = (_m.sin(dlat/2)**2 + _m.cos(lat1)*_m.cos(lat2)*_m.sin(dlon/2)**2)   #formula di haversine 
    return 2 * R * _m.asin(_m.sqrt(h))


# ====== KV helpers ======
async def kv_get(http: httpx.AsyncClient, k: str):
    """
    Legge una chiave dal KV.

    Args:
        http: HTTP del Client (con base_url già impostata).
        k: Nome della chiave (es. 'delivery:{id}', 'drone:{id}').

    Returns:
        Valore della chiave, oppure None se non esiste.
    """
    r = await http.get(f"/kv/{k}")                                              #richiesta get sul kvfront
    return None if r.status_code == 404 else r.json()["value"]                  #Se la chiave non esiste (404) restituisce None; altrimenti 
                                                                                #estrae il valore JSON nel campo "value".
async def kv_put(http: httpx.AsyncClient, k: str, v):
    """
    Scrive un valore nel KV via kvfront.

    Args:
        http: HTTP del Client.
        k: Nome della chiave.
        v: Valore da scrivere.

    Returns:
        None
    """
    await http.put(f"/kv/{k}", json={"value": v})                               #PUT su /kv/<chiave> con body JSON {"value": v}.

async def lock_acquire(http: httpx.AsyncClient, key: str, ttl=20) -> bool:
    """
    Tenta di acquisire un lock distribuito su una chiave.

    Args:
        http: HTTP del Client.
        key: Nome della chiave da lockare.
        ttl: TTL del lock in secondi.

    Returns:
        bool: True se il lock è stato acquisito, False altrimenti.
    """
    r = await http.post(f"/lock/acquire/{key}", params={"ttl_sec": ttl})        #prova ad acquisire un lock su key con scadenza automatica
    return bool(r.json().get("ok")) 

async def lock_release(http: httpx.AsyncClient, key: str):
    """
    Rilascia il lock distribuito su una chiave logica.

    Args:
        http: Client HTTP verso kvfront.
        key: Nome della chiave logica precedentemente lockata.

    Returns:
        None
    """
    await http.post(f"/lock/release/{key}")                                     # rilascia il lock su key

async def kv_cas(http: httpx.AsyncClient, key: str, old, new) -> bool:
    """
    Esegue un CAS sul primario della chiave.
    Confronta il valore corrente con 'old'; se coincidono, scrive 'new'.

    Args:
        http: HTTP del Client.
        key: Nome della chiave.
        old: Valore atteso corrente.
        new: Nuovo valore da scrivere.

    Returns:
        bool: True se il CAS è riuscito, False se la condizione non era soddisfatta o in caso d’errore.
    """
    try:
        r = await http.post("/kv/cas", json={"key": key, "old": old, "new": new})       #esegue il cas sul primario della chiave
        return bool(r.json().get("ok"))
    except Exception:
        return False
    

# ===== Helper di stato dei droni (per sbloccare situazioni incoerenti)=====

async def set_drone_idle_if_busy(http: httpx.AsyncClient, drone_id: str, expected_delivery: str, attempts: int = 40) -> bool:
    """
    Porta un drone da 'busy' a 'idle' se la consegna è terminata.
    Esegue retry brevi per assorbire race con la telemetria.

    Args:
        http: HTTP del Client.
        drone_id: ID del drone.
        expected_delivery: Delivery che il drone dovrebbe aver appena completato.
        attempts: Numero massimo di retry CAS.

    Returns:
        bool: True se la normalizzazione è riuscita o non necessaria; False se fallisce.
    """
    for _ in range(attempts):
        cur = await kv_get(http, f"drone:{drone_id}")                                   #legge lo stato del drone dal kv
        if not cur:
            return False                                                                # drone non trovato

                                                                                        # Se non è più busy su quella delivery, lo consideriamo risolto.
        if cur.get("status") != "busy" or cur.get("current_delivery") != expected_delivery:
            return True

                                                                                        #altrimenti facciamo la transizione busy→idle e sganciamo la delivery.
        new_doc = dict(cur)                                                             #crea una copia del documento del drone
        new_doc["status"] = "idle"
        new_doc["current_delivery"] = None

        if await kv_cas(http, f"drone:{drone_id}", cur, new_doc):                       #fa il CAS 
            return True

                                                                            #se non va a buon fine fa retry per il numero di attempts dopo un piccolo sleep 
        await asyncio.sleep(0.025)  
    return False


async def set_drone_busy_if_idle(http: httpx.AsyncClient, drone_id: str, delivery_id: str, attempts: int = 15) -> bool:
    """
    Porta un drone da 'idle' a 'busy' collegandolo a una delivery.
    Mantiene inalterati campi telemetrici.

    Args:
        http: HTTP del Client.
        drone_id: ID del drone.
        delivery_id: Delivery da assegnare al drone.
        attempts: Numero massimo di retry CAS.

    Returns:
        bool: True se la transizione è riuscita; False in caso di race/fallimento.
    """
    key = f"drone:{drone_id}"
    for _ in range(attempts):
        cur = await kv_get(http, key)                                               #prende il documento del drone dal kv
        if not cur:
            return False
                                                                                    # se NON è più idle, qualcuno ha già assegnato
        if cur.get("status") != "idle" or cur.get("current_delivery") is not None:
            return False
        
        new_doc = dict(cur)                                                         #crea una copia del documento del drone
        new_doc["status"] = "busy"
        new_doc["current_delivery"] = delivery_id
                                                                                    # non tocchiamo pos/battery/at_charge e altri campi che possono essere cambiati dal simulatore
        ok = await kv_cas(http, key, cur, new_doc)                                  #fa il CAS
        if ok:
            return True
        await asyncio.sleep(0.01)                                                   # 10ms backoff
    return False


async def reconcile_stuck_busy(http: httpx.AsyncClient):
    """
    Controllo periodico: se un drone resta 'busy' su una delivery già consegnata, lo riporta a 'idle'.

    Args:
        http: HTTP del Client.

    Returns:
        None
    """
    didx = await kv_get(http, "drones_index") or []                         #prende la lista dei droni nel kv
    for drone_id in didx:                                                   #itera su tutti i droni
        dr = await kv_get(http, f"drone:{drone_id}")                        #estrae il documento del drone dal kv 
        if not dr:
            continue
        if dr.get("status") == "busy" and dr.get("current_delivery"):
            did = dr["current_delivery"]
            dd = await kv_get(http, f"delivery:{did}")                      #legge il documento della consegna dal kv
            if dd and dd.get("status") == "delivered":
                                                                            # Forza la normalizzazione con il nostro helper (con retry esteso)
                await set_drone_idle_if_busy(http, drone_id, did)


# ====== funzioni geografiche ======
_ZCFG = None                                        #Variabile globale per cache in-process della configurazione delle zone (evita round-trip a KV ogni volta).
async def get_zcfg(http: httpx.AsyncClient):
    """
    Configurazione delle zone.

    Args:
        http: HTTP del Client.

    Returns:
        Oggetto zones_config o None se non disponibile.
    """
    global _ZCFG                                            #Usa la variabile globale per leggere/scrivere la cache.
    if _ZCFG is None:
        _ZCFG = await kv_get(http, "zones_config")          #carica la config delle zone dal kv dentro la variabile globale alla prima chiamata 
    return _ZCFG

def point_zone(zcfg, p):
    """
    Determina in quale zona cade un punto geografico.

    Args:
        zcfg: Configurazione delle zone.
        p: Dict con 'lat' e 'lon'.

    Returns:
        La zona che contiene il punto, altrimenti None.
    """
    for z in zcfg["zones"]:                                 #Scorre le zone definite in zcfg["zones"]
        b = z["bounds"]
        if b["lat_min"] <= p["lat"] <= b["lat_max"] and b["lon_min"] <= p["lon"] <= b["lon_max"]:
            return z
    return None

def nearest_charge_point(zcfg, p):
    """
    Trova il charge point più vicino a un punto.

    Args:
        zcfg: Configurazione delle zone.
        p: Punto di riferimento (dict con 'lat'/'lon').

    Returns:
        dict: Coordinate del charge point più vicino.
    """
    best, bestd = None, 1e18
    for z in zcfg["zones"]:
        cp = z["charge"]
        d = haversine_km(p, cp)
        if d < bestd:
            best, bestd = cp, d
    return best

def zone_proximity_rank(z_origin, z_drone):
    """
    Ranking di prossimità di zona tra origin e drone.

    Args:
        z_origin: Zona dell’origin.
        z_drone: Zona corrente del drone.

    Returns:
        int: 0 se stessa zona, 1 se adiacente, 2 altrimenti.
    """
    if not z_origin or not z_drone: return 2
    if z_origin["name"] == z_drone["name"]: return 0
    return 1 if z_drone["name"] in (z_origin.get("neighbors") or []) else 2

def pkg_class(weight: float) -> str:
    """
    Mappa il peso del pacco in classe richiesta ('light'/'medium'/'heavy').

    Args:
        weight: Peso del pacco in kg.

    Returns:
        str: Classe del pacco.
    """
    if weight <= 3:  return "light"
    if weight <= 7:  return "medium"
    return "heavy"

def classify_weight(delivery):
    """
    Restituisce la classe del pacco a partire dal documento delivery.

    Args:
        delivery: Documento della delivery (atteso campo 'weight').

    Returns:
        str: Classe del pacco.
    """
    return pkg_class(float(delivery.get("weight", 0.0)))

def can_complete_mission(drone, origin, destination, zcfg):
    """
    Verifica se la batteria del drone copre le distanze pos→origin→destination→nearest charge (+margine).

    Args:
        drone: Documento drone (richiede 'pos' e 'battery').
        origin: Punto di pickup (dict con 'lat'/'lon').
        destination: Punto di consegna (dict con 'lat'/'lon').
        zcfg: Configurazione delle zone (per trovare il charge point più vicino).

    Returns:
        tuple[bool, float, float]: (ok, km_totali, percentuale_batteria_richiesta)
    """

    pos = drone.get("pos")                                              #legge dal documento del drone in ingresso il parametro pos
    if not pos: return (False, 0.0, 0.0)
    d1 = haversine_km(pos, origin)
    d2 = haversine_km(origin, destination)
    charge_pt = nearest_charge_point(zcfg, destination)
    d3 = haversine_km(destination, charge_pt)
    total_km = d1 + d2 + d3
    required_pct = total_km * BATTERY_PER_KM + SAFETY_MARGIN_PCT
    battery_now = float(drone.get("battery", 0.0))                      #legge dal documento del drone in ingresso il parametro battery
    return (battery_now >= required_pct, total_km, required_pct)        #se riesce a completare la missione nella tupla restituisce un True 


# ====== pick_drone ======
async def pick_drone(http: httpx.AsyncClient, origin, destination, weight, delivery_id: str):
    """
    Seleziona il miglior drone 'idle' e idoneo per una consegna.

    Criteri: classe (in base al peso), batteria ≥ richiesta, distanza/zone (ranking),
    gestione early-charge tramite 'feas_miss' univoco per delivery.

    Args:
        http: HTTP del Client.
        origin: Dict coordinate di pickup {'lat','lon'}.
        destination: Dict coordinate di consegna {'lat','lon'}.
        weight: Peso del pacco (kg).
        delivery_id: ID della delivery (per tracciare i miss univoci).

    Returns:
        ID del drone selezionato, altrimenti None se nessun candidato idoneo.
    """
    zcfg = await get_zcfg(http)                                 #Carica (con cache in-process) la configurazione delle zone.
    z_origin = point_zone(zcfg, origin)                         #Determina in quale zona cade il punto origin (pickup).
    pclass = pkg_class(weight)                                  # Converte il peso in classe richiesta del drone: light/medium/heavy

    ids = await kv_get(http, "drones_index") or []              # legge la lista degli id dei droni
    candidates = []
    for did in ids:                                             #scorre i droni conosciuti
        d = await kv_get(http, f"drone:{did}")                  #carica il documento del singolo drone
        if not d: continue
        if d.get("status") != "idle": continue                  #va avanti solo se idle
        if d.get("current_delivery"):                           #va avanti solo se non ha una current delivery
            continue
        
        if (d.get("type") or "").lower() != pclass: continue   # filtra per classe coerente al peso del pacco

        if d.get("battery", 0.0) <= CRITICAL_BATTERY: 
            _log_charge("pick_drone:battery_critical", did,
                batt=d.get("battery", 0.0), crit=CRITICAL_BATTERY, miss=d.get("feas_miss", 0),
                ctx="pre-pick")                                 #aggiunta per vedere i motivi di ricarica dei droni
            d["status"] = "charging"                            #modifica il documento del drone
            await kv_put(http, f"drone:{did}", d)               #scrive sul kv 
            continue                                            #va avanti perchè il drone non va bene 

                                                                # Fattibilità energetica
        ok, _, _ = can_complete_mission(d, origin, destination, zcfg)
        if not ok:
                                                                # Incrementa miss SOLO se questa delivery non è ancora stata contata 
            miss_set = set(d.get("feas_miss_set") or [])
            miss = int(d.get("feas_miss", 0))

            if delivery_id not in miss_set:
                miss_set.add(delivery_id)
                miss += 1
                d_upd = dict(d)                                 #fa una copia del documento del drone per poterla aggiornare
                d_upd["feas_miss"] = miss 
                d_upd["feas_miss_set"] = list(miss_set)
                if miss >= EARLY_CHARGE_THRESHOLD:
                    d_upd["status"] = "charging"
                    d_upd["feas_miss"] = 0
                    d_upd["feas_miss_set"] = []
                    
                    print(f"[charge][pick_drone:feas_miss_threshold_unique] {did} miss={miss} thr={EARLY_CHARGE_THRESHOLD} (+{delivery_id})")
                else:
                    print(f"[charge][pick_drone:feas_miss_increment_unique] {did} miss={miss} thr={EARLY_CHARGE_THRESHOLD} (+{delivery_id})")
                await kv_put(http, f"drone:{did}", d_upd)           #carica l'aggiornamento del drone sul kv 
            else:
                                                                    # Già contata per questo drone -> no incremento
                print(f"[charge][pick_drone:feas_miss_duplicate] {did} already_seen={delivery_id} miss={miss}")

            continue

                                                                                    # Se la missione è fattibile: resetta il contatore e lo storico (se presenti)
        if d.get("feas_miss") or d.get("feas_miss_set"):
            d_ok = dict(d)                                                          #fa una copia del documento del drone per poterla aggiornare
            d_ok["feas_miss"] = 0
            d_ok["feas_miss_set"] = []
            await kv_put(http, f"drone:{did}", d_ok)                                #carica l'aggiornamento del drone sul kv
            print(f"[charge][pick_drone:feas_miss_reset] {did} reset due to feasible mission")

        dist_km   = haversine_km(d["pos"], origin)
        prox_rank = zone_proximity_rank(z_origin, point_zone(zcfg, d["pos"]))
        batt      = float(d.get("battery", 0.0))
        speed     = float(d.get("speed", 0.0))
        dist_bucket = int(dist_km / NEAR_EPS_KM) if NEAR_EPS_KM > 0 else 0
        candidates.append({
            "id": did, "dist_bucket": dist_bucket, "prox_rank": prox_rank,
            "battery": batt, "speed": speed, "dist_km": dist_km
        })

    if not candidates: return None
    candidates.sort(key=lambda x: (x["dist_bucket"], x["prox_rank"], x["battery"], -x["speed"]))            #ordina i droni secondo i criteri riportati
    best = candidates[0]
    if best["dist_km"] > MAX_PICKUP_KM:
        return None
    return best["id"]


# ====== pending più vecchie hanno precedenza ======
async def oldest_pending(http: httpx.AsyncClient, limit: int):
    """
    Ritorna le ID delle delivery in stato 'pending' cronologicamente più vecchie.

    Args:
        http: HTTP del Client..
        limit: Numero massimo di ID da restituire.

    Returns:
        list[str]: Lista di ID in stato pending ordinate per anzianità.
    """

    ids = await kv_get(http, "deliveries_index") or []          #Legge dal KV gli indice delle consegne (deliveries_index). L’ordine della lista corrisponde all’ordine di inserimento (first-in, first-out).
    out = []                                                    #Accumulatore delle id pending da restituire.
    for did in ids:                                             #Scorre tutti gli id nell’ordine in cui sono stati inseriti (dal più vecchio al più recente).
        if len(out) >= limit: break
        d = await kv_get(http, f"delivery:{did}")               #Carica il documento della singola delivery.
        if d and d.get("status") == "pending":                  #Se esiste ed è nello stato pending
            out.append(did)                                     #accoda l’id alla lista da restituire.
    return out



async def assign_round(http: httpx.AsyncClient, status_channel: aio_pika.Channel | None = None):
    """
    Tenta assegnazioni batch scorrendo le pending più vecchie (fairness).

    Args:
        http: HTTP del Client.
        status_channel: Canale AMQP per pubblicare delivery_status (un canale AMQP aperto su RabbitMQ).

    Returns:
        int: Numero di assegnazioni andate a buon fine in questo round.
    """
    ids = await oldest_pending(http, PENDING_SCAN_LIMIT)
    if not ids: return 0
    assigned = 0
    for did in ids:
        if assigned >= MAX_ASSIGN_PER_ROUND: break
        try:
            if await assign_one(http, did, status_channel=status_channel):
                assigned += 1
        except Exception as e:
            print(f"[dispatcher] error on {did}: {e}")
    if assigned:
        print(f"[dispatcher] round assigned={assigned}")
    return assigned

# ====== publish delivery_status (per pubblicare sulla coda delivery_status)======
async def publish_delivery_status(ch: aio_pika.Channel, event: dict):
    '''
    Funzione asincrona che pubblica un evento JSON sulla coda DELIVERY_STATUS: prende un dizionario event, lo trasforma in JSON e lo pubblica su RabbitMQ.
    Il contenuto del messaggio (il corpo) viene costruito nei punti dove viene chiamata la funzione    

    Args:
        ch: Canale AMQP aperto.
        event: un dizionario Python con le informazioni dell’evento (es. type, delivery_id, drone_id).

    Returns:
        None
    '''
    await ch.default_exchange.publish(                                      #Pubblica un messaggio sull’exchange di default del canale RabbitMQ.
        aio_pika.Message(                                                   #Crea il messaggio AMQP vero e proprio.
            body=json.dumps(event).encode("utf-8"),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json"                                 #aggiunge header al messaggio per dire che il corpo è in formato json
        ),
        routing_key=DELIVERY_STATUS_QUEUE,                                  #specifica il nome della coda destinataria  
    )


# ====== assegnazione (lock + CAS doppia con rollback) ======

async def assign_one(http: httpx.AsyncClient, delivery_id: str, status_channel: aio_pika.Channel | None = None) -> bool:
    """
    Prova ad assegnare una delivery a un drone (lock delivery + lock drone + CAS incrociato).

    Sequenza:
      1) Lettura delivery ('pending') e pick del drone
      2) Lock su delivery 
      3) Lock su drone, re-check fattibilità
      4) CAS drone: idle→busy + current_delivery
      5) CAS delivery: pending→assigned + drone_id + leg
      6) Pubblica evento 'delivery_assigned'; rollback in caso di fallimento CAS delivery.

    Args:
        http: HTTP del Client.
        delivery_id: ID della consegna da assegnare.
        status_channel: Canale AMQP su cui pubblicare l’evento (opzionale).

    Returns:
        bool: True se assegnazione riuscita, False altrimenti.
    """
                                                                        # lock sulla delivery (chiave *senza* prefisso 'lock:')
    dlock = f"delivery:{delivery_id}"                                   # costruisce la chiave di lock per la consegna.
    if not await lock_acquire(http, dlock, ttl=20):                     #prova a prendere il lock sulla delivery per 20 secondi. Se qualcun altro la sta già gestendo, abortisce subito.
        return False
    k_dlock = None
    try:
        ddoc = await kv_get(http, f"delivery:{delivery_id}")            #legge il documento della delivery dal kv
        if not ddoc or ddoc.get("status") != "pending":
            return False                                                 #se la consegna non esiste o non è più pending (già assegnata o completata),
                                                                                                #bisogna restituire false.

        origin, destination, weight = ddoc["origin"], ddoc["destination"], ddoc["weight"]       #estrazione dati principali della delivery.
        drone_id = await pick_drone(http, origin, destination, weight,delivery_id)              #si sceglie il drone migliore
        if not drone_id:
            return False                                                      #se non c'è nessun drone restituisce false 

                                                                        # lock sul drone scelto (chiave *senza* prefisso 'lock:'), PRIMA della rilettura/CAS
        k_dlock = f"drone:{drone_id}"                                   #costruisce la chiave di lock per il drone scelto.
        if not await lock_acquire(http, k_dlock, ttl=20):               #prova a prendere il lock sul drone per 20 secondi. Se qualcun altro la sta 
            return False                                                #già gestendo, abortisce subito.
        
                                                                        # ricontrollo immediato: batteria e fattibilità con telemetria più fresca
        sdoc = await kv_get(http, f"drone:{drone_id}")                  #legge dal kv il documento del drone
        if not sdoc:
            return False

                                                                        # Ricontrollo missione: pos->origin + origin->dest + dest->colonnina, con margine
        zcfg = await get_zcfg(http)
        feas_ok, _, _ = can_complete_mission(sdoc, origin, destination, zcfg)

                                                                        # Scarta se non fattibile o batteria sotto soglia critica
        if (not feas_ok) or float(sdoc.get("battery", 0.0)) <= CRITICAL_BATTERY:
            batt_now = float(sdoc.get("battery", 0.0))
            miss = int(sdoc.get("feas_miss", 0)) + 1

            miss_set= set(sdoc.get("feas_miss_set") or [])
            if delivery_id not in miss_set:
                miss_set.add(delivery_id)

            s_upd = dict(sdoc); 
            s_upd["feas_miss"] = miss
            
            s_upd["feas_miss_set"]=miss_set

            if batt_now <= CRITICAL_BATTERY:
                _log_charge("assign_one:battery_critical", drone_id,
                            batt=batt_now, crit=CRITICAL_BATTERY)

            if not feas_ok:
                if miss >= EARLY_CHARGE_THRESHOLD:
                    _log_charge("assign_one:feas_miss_threshold", drone_id,
                                miss=miss, thr=EARLY_CHARGE_THRESHOLD)
                else:
                    _log_charge("assign_one:feas_miss_increment", drone_id,
                                miss=miss, thr=EARLY_CHARGE_THRESHOLD)

            if batt_now <= CRITICAL_BATTERY or miss >= EARLY_CHARGE_THRESHOLD:
                s_upd["status"] = "charging"

                if miss >= EARLY_CHARGE_THRESHOLD:
                    s_upd["feas_miss"] = 0
                    s_upd["feas_miss_set"] = []

            await kv_put(http, f"drone:{drone_id}", s_upd)
            return False

                                                                                        # claim del drone con retry/merge (idle -> busy/current_delivery)
        ok_busy = await set_drone_busy_if_idle(http, drone_id, delivery_id)             #prova a portare il drone busy assegnando la richiesta 
        if not ok_busy:
            return False                                                                # perso race o drone non più idle

                                                                                        # CAS: delivery pending -> assigned
        d_old = ddoc
        d_new = dict(d_old)                                                             #copia del documento della delivery
        d_new.update({"status": "assigned", "drone_id": drone_id, "leg": "to_origin"})
        if not await kv_cas(http, f"delivery:{delivery_id}", d_old, d_new):             #prova a scrivere il nuovo stato della delivery (pending -> assigned). 
            await set_drone_idle_if_busy(http, drone_id, delivery_id)                   #Se fallisce, fa rollback del drone -> idle, così non resta bloccato busy inutilmente.
                                                                                        # rollback drone -> idle (best-effort, con retry)
            return False

        print(f"[dispatcher] ASSIGNED {delivery_id} -> {drone_id}")
        if status_channel:
            await publish_delivery_status(status_channel, {                             #pubblica il messaggio sulla coda delivery assigned 
                "type": "delivery_assigned",
                "delivery_id": delivery_id,
                "drone_id": drone_id
            })   
        return True

    finally:
        if k_dlock:
            await lock_release(http, k_dlock)
        await lock_release(http, dlock)

# ====== avanzamento consegne ======
async def advance_deliveries(http: httpx.AsyncClient, status_channel: aio_pika.Channel | None = None):

    """
    Avanza in batch, periodicamente, lo stato delle consegne attive.

    Transizioni:
      - assigned -> in_flight (al primo movimento)
      - leg: to_origin -> to_destination
      - in_flight -> delivered (+ normalizzazione drone a idle)

    Args:
        http: HTTP del Client.
        status_channel: Canale AMQP per 'delivery_completed' (opzionale).

    Returns:
        None
    """
    ids = await kv_get(http, "deliveries_index") or []              #Legge gli indici delle delivery per iterarle.
    progressed = 0
    for did in ids:                                                 # Scorre le consegne 
        d = await kv_get(http, f"delivery:{did}")                   #carica il documento della delivery dal kv
        if not d: continue
        st = d.get("status")                                        #prende lo stato della consegna 
        if st not in ("assigned", "in_flight"): continue            #se non è assigned o in flight cambia consegna 

        drone_id = d.get("drone_id")                                #prende il drone associato alla delivery
        if not drone_id: continue                                   #se non esiste esce 
        s = await kv_get(http, f"drone:{drone_id}")                 #carica dal kv il documento del drone
        if not s: continue                                          #se non esiste continua
        pos = s.get("pos")                                          #estrae la posizione
        if not pos: continue

        if st == "assigned":                                        #Appena vede un drone con pos valida su una delivery assigned, la porto a in_flight via CAS.
            d_new = dict(d); d_new["status"] = "in_flight"
            await kv_cas(http, f"delivery:{did}", d, d_new)         #faccio il cas dell'aggiornamento
            d = d_new 

        leg = (d.get("leg") or "to_origin")
        if leg == "to_origin":
            if haversine_km(pos, d["origin"]) <= ARRIVE_EPS_KM:
                d_new = dict(d); d_new["leg"] = "to_destination"
                await kv_cas(http, f"delivery:{did}", d, d_new); d = d_new; progressed += 1         #faccio il cas dell'aggiornamento
        else:
            if haversine_km(pos, d["destination"]) <= ARRIVE_EPS_KM:
                                                                                                    # delivery -> delivered
                d_new = dict(d); d_new["status"] = "delivered"; d_new["leg"] = None
                await kv_cas(http, f"delivery:{did}", d, d_new)                                     #faccio il cas dell'aggiornamento
                await set_drone_idle_if_busy(http, drone_id, did)                                   #passa il drone in idle
                progressed += 1

                if status_channel:
                    await publish_delivery_status(status_channel, {                                 # pubblica l'evento delivery completed
                        "type": "delivery_completed",
                        "delivery_id": did,
                        "drone_id": drone_id
                    })   
    if progressed:
        print(f"[dispatcher] progressed={progressed}")

# ====== avanzamento mirato (dopo evento broker) ======
async def advance_for_drone(http, status_channel, drone_id: str):
    """
    Avanza lo stato della singola delivery legata al drone indicato.
    È la versione “mirata” chiamata quando arriva un evento su 'drone_updates'.

    Args:
        http: HTTP del Client.
        status_channel: Canale AMQP per 'delivery_completed' (opzionale).
        drone_id: ID del drone di interesse.

    Returns:
        None
    """
    s = await kv_get(http, f"drone:{drone_id}")                              
    if not s: return
    did = s.get("current_delivery")
    if not did: return
    d = await kv_get(http, f"delivery:{did}")
    if not d: return

    st = d.get("status")
    if st not in ("assigned","in_flight"): return
    pos = s.get("pos")
    if not pos: return

    if st == "assigned":
        d_new = dict(d); d_new["status"] = "in_flight"
        await kv_cas(http, f"delivery:{did}", d, d_new); d = d_new

    leg = (d.get("leg") or "to_origin")
    if leg == "to_origin":
        if haversine_km(pos, d["origin"]) <= ARRIVE_EPS_KM:
            d_new = dict(d); d_new["leg"] = "to_destination"
            await kv_cas(http, f"delivery:{did}", d, d_new)
    else:
        if haversine_km(pos, d["destination"]) <= ARRIVE_EPS_KM:
            d_new = dict(d); d_new["status"] = "delivered"; d_new["leg"] = None
            await kv_cas(http, f"delivery:{did}", d, d_new)
            await set_drone_idle_if_busy(http, drone_id, did)
            if status_channel:
                await publish_delivery_status(status_channel, {
                    "type":"delivery_completed","delivery_id":did,"drone_id":drone_id
                })

# ====== charging/retiring ======
async def govern_charging_and_retiring(http: httpx.AsyncClient):
    """
    Gestisce le transizioni di stato legate alla carica/ritiro dei droni.

    Transizioni:
      - charging → idle (batteria≥FULL_AFTER e at_charge=True)
      - retiring → inactive (stesse condizioni)
      - idle → charging (batteria≤CRITICAL_BATTERY)

    Args:
        http: HTTP del Client.

    Returns:
        None
    """
    ids = await kv_get(http, "drones_index") or []                          #legge la lista degli indici dei droni dal kv
    changed = 0
    for did in ids:
        d = await kv_get(http, f"drone:{did}")                              #prende il documento del drone dal kv
        if not d: continue
        st = d.get("status")                                                #estare il campo status

        if st == "charging":
            if bool(d.get("at_charge")) and d.get("battery", 0.0) >= FULL_AFTER:
                                                                                    # CAS con retry breve: assorbe conflitti con telemetria del drone_sim
                for _ in range(5):
                    cur = await kv_get(http, f"drone:{did}")                        #prende il documento del drone corrente
                    if not cur or cur.get("status") != "charging":
                        break
                    if not (bool(cur.get("at_charge")) and cur.get("battery", 0.0) >= FULL_AFTER):
                        break
                    new_doc = dict(cur); new_doc["status"] = "idle"
                    if await kv_cas(http, f"drone:{did}", cur, new_doc):            #prova a fare il cas per aggiornare
                        changed += 1
                        break
                    await asyncio.sleep(0.01)                                       # 10ms di sleep e poi ci riprova
        elif st == "retiring":
            if bool(d.get("at_charge")) and d.get("battery", 0.0) >= FULL_AFTER:
                                                                                    # CAS con retry breve: evita che resti “retiring” per conflitti di scrittura
                for _ in range(5):
                    cur = await kv_get(http, f"drone:{did}")
                    if not cur or cur.get("status") != "retiring":
                        break
                    if not (bool(cur.get("at_charge")) and cur.get("battery", 0.0) >= FULL_AFTER):
                        break
                    new_doc = dict(cur); new_doc["status"] = "inactive"
                    if await kv_cas(http, f"drone:{did}", cur, new_doc):            #prova a fare il cas per aggiornare
                        changed += 1
                        break
                    await asyncio.sleep(0.01)           # 10ms
        elif st == "idle":
            if d.get("battery", 0.0) <= CRITICAL_BATTERY:
                _log_charge("guard_idle:battery_critical", did,
                            batt=d.get("battery", 0.0), crit=CRITICAL_BATTERY)
                d_new = dict(d); d_new["status"] = "charging"
                await kv_cas(http, f"drone:{did}", d, d_new); changed += 1
    if changed:
        print(f"[dispatcher] charge/retire transitions={changed}")


# ====== autoscaling ======

async def autoscale_by_type(http: httpx.AsyncClient):
    """
    Calcola quali droni attivare in base al numero di consegne "pending" (mantenendo le categorie di peso).

    Args:
        http: HTTP del Client.

    Returns:
        None
    """
    idx = await kv_get(http, "deliveries_index") or []                                  #prende gli indici delle deliveries dal kv
    pending = [await kv_get(http, f"delivery:{i}") for i in idx]                        #fa una lista contenente i documenti delle deliveries prese dal kv 
    pending = [d for d in pending if d and d.get("status") == "pending"]                #sovrascrive pending mettendo dentro solo le deliveries pending 
    per_type = {"light": 0, "medium": 0, "heavy": 0}
    for d in pending:                                                                   #scorre tutti i documenti delle richieste 
        per_type[classify_weight(d)] += 1                                               #per ogni richiesta tramite la funzione classify_weight estrae 
                                                                                        # il peso dal documento e calcola il tipo del pacco
    backlog = sum(per_type.values())                                            #fa la somma dei vari valori nel dict (quindi consegne leggere+medium+heavy)
    target_total = max(BASE_ACTIVE, min(DRONE_POOL_MAX, math.ceil(backlog * SCALE_RATIO)))          #calcola il numero totale di droni attivo desiderato
                                                                                #math.ceil(backlog * SCALE_RATIO) è il numero di droni necessari per le consegne 
                                                                                #pending trovate arrotondato per eccesso

    didx = await kv_get(http, "drones_index") or []                                             #estrae la lista dei droni dal kv
    buckets = {"light":{"idle":[], "busy":[], "charging":[], "retiring":[], "inactive":[]},
               "medium":{"idle":[], "busy":[], "charging":[], "retiring":[], "inactive":[]},
               "heavy":{"idle":[], "busy":[], "charging":[], "retiring":[], "inactive":[]}}     #per ogni tipo conta quanti droni ci sono in ogni stato
    for did in didx:                                                                            #itera sugli id dei droni
        d = await kv_get(http, f"drone:{did}")                                                  #estrae i documenti del drone dal kv 
        if not d: continue
        t = (d.get("type") or "light").lower()
        s = d.get("status", "inactive")
        if t not in buckets: t = "light"
        if s not in buckets[t]: s = "inactive"
        buckets[t][s].append(did)                                               #l'id di quel drone viene messo al posto giusto in bucket 

    if backlog == 0:
        base = BASE_ACTIVE // 3                                                 #divisione intera per ottenere la quota di base per ogni classe.
        rem  = BASE_ACTIVE - base*3
        order = ["light", "medium", "heavy"]
        target_by_type = {k: base for k in order}
        for k in order[:rem]:
            target_by_type[k] += 1
    else:
        target_by_type = {}
        for k, v in per_type.items():
            share = v / backlog if backlog > 0 else 0                           #frazione di consegne di un tipo sul totale
            target_by_type[k] = max(0, round(share * target_total))             #frazione di droni totali che dovranno essere idle per soddisfare
                                                                                # quel determinato tipo
    actions = []
    for t in ("light", "medium", "heavy"):
        active_now = len(buckets[t]["idle"]) + len(buckets[t]["busy"]) + len(buckets[t]["charging"])
        total_now  = active_now + len(buckets[t]["retiring"])
        target     = target_by_type.get(t, 0)                               #il numero di droni che deve esistere (quelli già presenti e da aggiungere) 
        if active_now < target:                                             #per soddisfare le richieste di quel tipo
            need = target - active_now
            take = min(need, len(buckets[t]["inactive"]))
                                                                            # ATTIVAZIONE sotto mutex per evitare contrasti con l'assegnazione
            async with SCHED_LOCK:
                for did in buckets[t]["inactive"][:take]:
                    d = await kv_get(http, f"drone:{did}")                  #estrae il documento del drone 
                    if not d: 
                        continue
                                                                            # attiva solo se è ancora davvero inactive
                    if d.get("status") != "inactive":
                        continue
                    d_new = dict(d); d_new["status"] = "idle"
                    await kv_cas(http, f"drone:{did}", d, d_new)            #fa il cas per l'aggiornamento
                    actions.append(f"activate {did}")

        elif active_now > target:
            noneed = active_now - target                                    #il numero di droni che si dovrebbero spegnere
            pool = buckets[t]["charging"] + buckets[t]["idle"]              #abbiamo la lista dei droni del tipo analizzato che sono charging o idle 
                                                                            # RITIRO sotto mutex; MAI se c'è current_delivery
            async with SCHED_LOCK:
                safe_pool = []
                for did in pool:                                            #itera sugli id dei droni in pool
                    d = await kv_get(http, f"drone:{did}")                  #estrae il documento del drone dal kv
                    if not d:
                        continue
                                                                            # barriera monotona: non ritirare se legato a una consegna
                    if d.get("current_delivery"):
                        continue
                                                                            # evita comunque i busy
                    if d.get("status") == "busy":
                        continue
                    safe_pool.append((did, d))                              #qua ci sono i droni che effettivamente si possono spegnere 

                take = min(max(noneed, 0), len(safe_pool))              #prende il numero minimo tra quelli che teoricamnete si devono spegnere e quelli che si possono spegnere 
                for did, d in safe_pool[:take]:                         #itera tra i droni che si possono spegnere solo fino al numero necessario
                    d_new = dict(d); d_new["status"] = "retiring"
                    await kv_cas(http, f"drone:{did}", d, d_new)        #fa il cas per l'aggiornamento
                    actions.append(f"retire {did}")

    if actions:
        print("[dispatcher][autoscale]", "; ".join(actions))


async def inconsistency_guard(http: httpx.AsyncClient):
    """
    Stampa incongruenze tra stato e current_delivery dei droni. 
    Serve per loggare periodicamente stati drone incoerenti (aiuto per il debug).

    Regola:
      - 'busy' deve avere current_delivery != None
      - 'idle' deve avere current_delivery == None

    Args:
        http: HTTP del Client.

    Returns:
        None
    """
    while True:
        try:
            didx = await kv_get(http, "drones_index") or []
            inconsistent = []
            for did in didx:
                dr = await kv_get(http, f"drone:{did}")
                if not dr: continue
                cur = dr.get("current_delivery")
                st  = dr.get("status")
                                                                                        # busy deve avere current_delivery; idle non deve averlo
                if (st == "busy" and not cur) or (st == "idle" and cur):
                    inconsistent.append(f"{did}: status={st} cur={cur}")
            if inconsistent:
                print("[guard][drone] inconsistent:", "; ".join(inconsistent))
        except Exception as e:
            print("[guard] error:", e)
        await asyncio.sleep(10)
    



# ====== Consumers broker ======
async def start_consumers(http: httpx.AsyncClient, channel: aio_pika.Channel):
    """
    Dichiara le code e attacca i consumer per delivery_requests e drone_updates.

    Args:
        http: HTTP del Client.
        channel: Canale AMQP già aperto.

    Returns:
        None
    """
    q = await channel.declare_queue(DELIVERY_REQ_QUEUE, durable=True)               # idempotente
    await channel.set_qos(prefetch_count=32)
    await channel.declare_queue(DELIVERY_REQ_QUEUE, durable=True)
    await channel.declare_queue(DRONE_UPDATES_QUEUE, durable=True)
    await channel.declare_queue(DELIVERY_STATUS_QUEUE, durable=True)
                                                                                    # dichiara le tre code 

    print("[dispatcher] consumer for DELIVERY_REQ_QUEUE attached (on_request)")


    delivery_q = await channel.get_queue(DELIVERY_REQ_QUEUE)
    drone_q    = await channel.get_queue(DRONE_UPDATES_QUEUE)
                                    #dal canale AMQP (channel) chiede ad aio_pika di restituire un oggetto Python che rappresenta quella coda su RabbitMQ.

    async def on_request(message: aio_pika.IncomingMessage):
        """
        Callback per messaggi su delivery_requests: tenta l'assegnazione immediata.

        Args:
            message: Messaggio AMQP con payload JSON (delivery_id, origin, destination, weight).

        Returns:
            None
        """
        async with message.process(ignore_processed=True):
            try:
                payload = json.loads(message.body.decode("utf-8"))
            except Exception:
                return
            did = payload.get("delivery_id")
            if not did: return
            await assign_one(http, did, status_channel=channel)                         #richiama assign_one

    async def on_drone_upd(message: aio_pika.IncomingMessage):
        """
        Callback per messaggi su drone_updates: avanza lo stato della consegna del drone.

        Args:
            message: Messaggio AMQP con payload JSON (drone_id, pos, battery, ...).

        Returns:
            None
        """
        async with message.process(ignore_processed=True):
            try:
                payload = json.loads(message.body.decode("utf-8"))
            except Exception:
                return
            drone_id = payload.get("drone_id")
            if not drone_id: return
            await advance_for_drone(http, channel, drone_id)        #richiama advance for drone 

    await delivery_q.consume(on_request, no_ack=False)              #quando arriva un messaggio sulla coda delivery request esegue la funzione in ingresso
    await drone_q.consume(on_drone_upd, no_ack=False)               #quando arriva un messaggio sulla coda drone updates deve esegue la funzione in ingresso
    print("[dispatcher] consumers started")

# ====== Scheduler ======
async def scheduler_loop(http: httpx.AsyncClient, channel: aio_pika.Channel):
    """
    Loop periodico di gestione: autoscale, charging/retiring, avanzamento e assegnazioni.

    Args:
        http: HTTP del Client.
        channel: Canale AMQP per pubblicazioni su delivery_status.

    Returns:
        None
    """
    tick = ASSIGNER_TICK_MS / 1000.0
    print(f"[dispatcher] scheduler every {tick:.3f}s")
    AUTOSCALE_DISABLED = os.getenv("AUTOSCALE_DISABLED", "0") == "1"
    while True:
        try:
            if not AUTOSCALE_DISABLED:
                await autoscale_by_type(http)
            await govern_charging_and_retiring(http)
            await advance_deliveries(http, status_channel=channel)
            await reconcile_stuck_busy(http)
            await assign_round(http, status_channel=channel)
        except Exception as e:
            print(f"[dispatcher] scheduler error: {e}")
        await asyncio.sleep(tick)



# ====== main ======
async def main():
    """
    Entry-point: connessione a RabbitMQ, avvio consumer e scheduler loop.

    Returns:
        None
    """
    print("[dispatcher] starting (async)…")
    connection = await aio_pika.connect_robust(RABBIT_URL)
    channel    = await connection.channel()
    await channel.set_qos(prefetch_count=20)
    await channel.declare_queue(DELIVERY_REQ_QUEUE, durable=True)
    await channel.declare_queue(DRONE_UPDATES_QUEUE, durable=True)
    await channel.declare_queue(DELIVERY_STATUS_QUEUE, durable=True)
    print("[dispatcher] connected to RabbitMQ + queues declared")

    async with httpx.AsyncClient(base_url=KV_URL, timeout=5.0) as http:
        await start_consumers(http, channel)
        asyncio.create_task(inconsistency_guard(http))
        await scheduler_loop(http, channel)

if __name__ == "__main__":
    asyncio.run(main())