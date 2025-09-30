#   per ogni drone loop con KV update garantito (CAS) + publisher
# - Ogni drone: ad ogni tick legge KV, calcola pos/battery/at_charge e scrive nel KV
#   usando CAS+retry (non sovrascrive status/current_delivery/type/speed).
# - L’evento broker è decoupled: messo in coda e pubblicato da un task dedicato.
# - Step frazionale (speed pure più fluida sulla dashboard).
# - Se la coda eventi è piena, si scarta il più vecchio.

import os, random, math, json, asyncio
import httpx
import aio_pika
import time


KV_URL = os.getenv("KV_URL","http://kvfront:9000")                          #URL base del servizio kvfront
RABBIT_URL = os.getenv("RABBIT_URL","amqp://guest:guest@rabbitmq/")         #Legge la variabile d’ambiente RABBIT_URL; se assente, default all’URL AMQP del broker RabbitMQ interno al compose.
DRONE_UPDATES_QUEUE = os.getenv("DRONE_UPDATES_QUEUE", "drone_updates")     #coda AMQP dove il drone_sim pubblica aggiornamenti dei droni

CHARGE_PER_TICK = float(os.getenv("CHARGE_PER_TICK","5.0"))                 # % per tick quando in carica
BATTERY_PER_KM  = float(os.getenv("BATTERY_PER_KM", "1.2"))                 # % per km percorso
TICK_SEC        = float(os.getenv("DRONE_TICK_SEC", "0.05"))
HTTP_TIMEOUT    = float(os.getenv("HTTP_TIMEOUT","3.0"))

TYPE_PATTERN = [("light", 0.40), ("medium", 0.25), ("heavy", 0.15)]         #Dizionario con tipo e rispettiva velocità dei droni
EVENT_QUEUE_MAX = int(os.getenv("EVENT_QUEUE_MAX", "2000"))                 #Dimensione massima della coda locale degli eventi da pubblicare

# ===== util =====
def step(p,q,f):
    """
    Calcola un punto intermedio tra p e q usando una frazione f, per far muovere i droni.

    Args:
      p (dict): punto di partenza {"lat": float, "lon": float}.
      q (dict): punto di arrivo   {"lat": float, "lon": float}.
      f (float): frazione [0..1] di avanzamento lungo il segmento p→q.

    Returns:
      dict: nuovo punto {"lat": float, "lon": float}.
    """
    return {"lat": p["lat"]+(q["lat"]-p["lat"])*f,
            "lon": p["lon"]+(q["lon"]-p["lon"])*f}

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
    lat1, lon1 = math.radians(a["lat"]), math.radians(a["lon"])
    lat2, lon2 = math.radians(b["lat"]), math.radians(b["lon"])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    h = (math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2)
    return 2 * R * math.asin(math.sqrt(h))

def close_enough(p,q,eps=0.0005):
    """
    Verifica che due punti siano “sufficientemente vicini” su lat/lon.

    Args:
      p (dict): {"lat": float, "lon": float}.
      q (dict): {"lat": float, "lon": float}.
      eps (float): soglia assoluta su lat e lon.

    Returns:
      bool: True se |Δlat|<eps e |Δlon|<eps.
    """
    return abs(p["lat"]-q["lat"]) < eps and abs(p["lon"]-q["lon"]) < eps

# ===== KV helpers (client http passato come parametro esplicito)=====
async def kv_get(c: httpx.AsyncClient, k: str):
    """
    Legge una chiave dal KV-front.

    Args:
      c (httpx.AsyncClient): HTTP del client.
      k (str): chiave logica.

    Returns:
      Any | None: valore associato (campo "value") oppure None se 404.
    """
    r = await c.get(f"/kv/{k}")
    return None if r.status_code == 404 else r.json()["value"]

async def kv_put(c: httpx.AsyncClient, k: str, v):
    """
    Scrive il valore di una chiave nel KV-front.

    Args:
      c (httpx.AsyncClient): HTTP del client.
      k (str): chiave logica.
      v (Any): valore (serializzabile JSON).
    """
    await c.put(f"/kv/{k}", json={"value": v})

async def kv_cas(c: httpx.AsyncClient, key: str, old, new) -> bool:
    """
    Confronta e sostituisce sul KV-front (operazione CAS).

    Args:
      c (httpx.AsyncClient): client HTTP
      key (str): chiave logica.
      old (Any): valore atteso attuale.
      new (Any): nuovo valore da scrivere se il confronto ha successo.

    Returns:
      bool: True se il CAS è riuscito, False altrimenti.
    """
    try:
        r = await c.post("/kv/cas", json={"key": key, "old": old, "new": new})
        return bool(r.json().get("ok"))
    except Exception:
        return False

# ===== domain =====
async def get_zcfg(c):
    """
    Recupera la configurazione delle zone.

    Args:
      c (httpx.AsyncClient): HTTP del client.

    Returns:
      dict | None: oggetto zones_config oppure None se mancante.
    """
    return await kv_get(c, "zones_config")

def nearest_charge_point(zcfg, p):
    """
    Trova il charge point più vicino a un punto p.

    Args:
      zcfg (dict): configurazione zone.
      p (dict): punto {"lat": float, "lon": float}.

    Returns:
      dict: coordinate del charge point più vicino.
    """
    best, bestd = None, 1e18
    for z in zcfg["zones"]:
        cp = z["charge"]; d = haversine_km(p, cp)
        if d < bestd: best, bestd = cp, d
    return best

def build_types(n):
    """
    Genera una lista ciclica di tipi/velocità per n droni, in modo da ottenere una 
    distribuzione pseudo-equilibrata di light/medium/heavy e delle rispettive velocità.

    Args:
      n (int): numero droni.

    Returns:
      list[tuple[str,float]]: coppie (type, step_fraction_per_tick).
    """
    types=[]; k=len(TYPE_PATTERN); start=random.randrange(k)
    for i in range(n): types.append(TYPE_PATTERN[(start+i)%k])
    return types

async def register_pool(c, zcfg, n_total=18):
    """
    Registra/aggiorna nel KV l'intero pool di droni.

    Args:
      c (httpx.AsyncClient): HTTP del client.
      zcfg (dict): configurazione zone (usata per inizializzare posizioni).
      n_total (int): numero massimo di droni da registrare.

    Returns:
      None
    """
    idx = await kv_get(c, "drones_index") or []                     #inizializza la lista degli indici dei droni se è la prima chiamata o se no la legge dal kv
    types = build_types(n_total)                                    #restituisce una lista di droni composta da tipo e velocità corrispondente
    charges=[z["charge"] for z in zcfg["zones"]];                   #zcfg[zones] è una lista di dizionari, ognuno rappresenta una zona, e di ogni zona si estrae il campo charge
                                                                    #che è un dict con le coordinate del punto di ricarica di quella zona.
    random.shuffle(charges)                                         #Mischia la lista dei charge point in ordine casuale.
                                                                    #Serve per non piazzare sempre i droni nello stesso schema ripetitivo, ma distribuire le posizioni iniziali in modo random.
    
    
    m=len(charges)

    for i in range(n_total):                                        #Crea ID sequenziali (drone-1, drone-2, …). Assegna tipo e velocità.Posizione iniziale: vicino a un charge point (con piccolo shift casuale).
        did = f"drone-{i+1:01d}"
        if did not in idx: idx.append(did)
        dtype, speed = types[i]                                     # speed = frazione step 
        pos = charges[i % m].copy()
        pos["lat"] += random.uniform(-0.0004, 0.0004)
        pos["lon"] += random.uniform(-0.0004, 0.0004)
        d = await kv_get(c, f"drone:{did}") or {}                   # inizializza (o preserva se già presenti) i campi chiave del drone.
        d.update({
            "id": did,
            "type": dtype,
            "status": d.get("status","inactive"),                   # status parte inactive, sarà l’autoscaling del dispatcher ad attivarli.
            "battery": d.get("battery", 100.0),
            "pos": d.get("pos", pos),
            "speed": float(speed),                                  # fraction-per-tick
            "current_delivery": d.get("current_delivery"),
            "feas_miss": int(d.get("feas_miss",0)),
            "at_charge": bool(d.get("at_charge", False))
        })
        await kv_put(c, f"drone:{did}", d)                          #scrive sul kv il drone
    await kv_put(c, "drones_index", idx)                            #scrive nella lista dei droni il drone appena registrato
    print(f"[drone_sim] pool registered: total={len(idx)}")


# ===== publisher dedicato (decoupled) (il drone loop spinge eventi nella coda locale senza bloccare; il publisher li svuota e li manda al broker)=====
async def publisher_task(evt_q: asyncio.Queue):
    """
    Task dedicato alla pubblicazione su RabbitMQ degli eventi in coda. E' una coroutine che prende una coda asincrona evt_q 
    da cui leggere gli eventi da pubblicare su RabbitMQ.

    - Mantiene connessione/canale robusti con backoff progressivo.
    - Consuma eventi da `evt_q` e li pubblica sulla coda DRONE_UPDATES.
    - Non blocca il loop dei droni in caso di problemi di broker.

    Args:
      evt_q (asyncio.Queue): coda con dict evento pronti per il broker. 
                            (la funzione non lavora direttamente con RabbitMQ, ma con una coda asincrona interna a Python)

    Returns:
      None (loop infinito).
    """
    connection = None
    channel = None
    queue_declared = False
    backoff = 1.0

    def _mk_msg(evt: dict) -> aio_pika.Message:
        """
        Costruisce un messaggio AMQP pronto per la pubblicazione su RabbitMQ.

        Args:
            evt (dict): Dizionario che rappresenta l'evento da inviare
                (es. drone_id, posizione, batteria, stato, ecc.).

        Returns:
            aio_pika.Message: Messaggio AMQP serializzato in JSON, con:
                - body: stringa JSON codificata UTF-8
                - content_type: "application/json"
                - delivery_mode: PERSISTENT (il broker lo salva su disco)
        """
        return aio_pika.Message(
            body=json.dumps(evt).encode("utf-8"),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )

    while True:
        try:
            if channel is None or channel.is_closed:                                                # (Ri)connessione se serve
                if connection is None or connection.is_closed:
                    connection = await aio_pika.connect_robust(RABBIT_URL)                          #crea una connessione verso Rabbit
                channel = await connection.channel()                                                #Apre un canale sulla connessione.
                await channel.set_qos(prefetch_count=256)
                if not queue_declared:
                    await channel.declare_queue(DRONE_UPDATES_QUEUE, durable=True)                  #Dichiara la coda drone_updates una sola volta
                    queue_declared = True
                backoff = 1.0

            
            evt = await evt_q.get()                                                                 # prendi un evento e pubblica
            try:
                await channel.default_exchange.publish(_mk_msg(evt), routing_key=DRONE_UPDATES_QUEUE)   #Pubblica il messaggio sull’exchange di default 
            finally:                                                                                    #con routing_key (instrada il messaggio alla coda con lo stesso nome della routing_key.)
                evt_q.task_done()                                                                       #Segnala alla coda locale che l’evento è stato processato

        except Exception as e:                                                                    #il publisher gestisce i problemi senza bloccare 
            print("[drone_sim][publisher] WARN:", type(e).__name__, e)                            #i loop dei droni perché è in task separata e legge da evt_q. 
            await asyncio.sleep(backoff)                                                          #Se il broker è lento/giù, gli eventi si accodano
            backoff = min(5.0, backoff * 1.7)

# ===== per-drone loop =====
async def run_one(did: str, c: httpx.AsyncClient, zcfg: dict, evt_q: asyncio.Queue):
    """
    Pipeline principale per un singolo drone. E' una coroutine che simula un singolo drone:

    - Legge stato attuale del drone dal KV.
    - Se busy: avanza verso origin/destination e consuma batteria.
    - Se charging/retiring: si muove verso la colonnina e poi ricarica.
    - Scrive pos/battery/at_charge nel KV con CAS+retry (merge-minimal).
    - Messa in coda dell'evento di telemetria.

    Args:
      did (str): id del drone (es. "drone-1").
      c (httpx.AsyncClient): HTTP del client.
      zcfg (dict): configurazione zone.
      evt_q (asyncio.Queue): coda per inviare gli eventi al publisher.

    Returns:
      None (loop infinito).
    """
    try:
        d0 = await kv_get(c, f"drone:{did}") or {}
        print(f"[drone {did}] ready (type={d0.get('type')}, speed={d0.get('speed')})")
    except Exception as e:
        print(f"[drone {did}] init warn:", e)

    while True: 
        try:
            cur = await kv_get(c, f"drone:{did}")                                       #legge lo stato attuale del drone dal kv 
            
            if cur and cur.get("freeze_until", 0) > time.time():
                                                                                        # pausa breve e salta questo tick per evitare CAS con il dispatcher
                await asyncio.sleep(0.05)
                continue

            if not cur:
                await asyncio.sleep(TICK_SEC); continue                                 #se non esiste il documento del drone attende un tick e riprova

            status = cur.get("status","inactive")            
            current_delivery = cur.get("current_delivery")    
            speed = float(cur.get("speed", 0.25))                                       # fraction-per-tick
            pos = cur.get("pos")
            if not pos:
                await asyncio.sleep(TICK_SEC); continue

            new_pos = dict(pos)
            new_battery = float(cur.get("battery", 100.0))
            at_charge = False

            if status == "busy" and current_delivery:
                dd = await kv_get(c, f"delivery:{current_delivery}")                    #prende dal kv il documento della current delivery
                if dd:
                    leg = dd.get("leg","to_origin")
                    target = dd["origin"] if leg == "to_origin" else dd["destination"]  #setta il target nel drone 
                    prev = pos
                    new_pos = step(prev, target, speed)
                    km = haversine_km(prev, new_pos)
                    new_battery = max(0.0, new_battery - km*BATTERY_PER_KM)

            elif status in ("charging","retiring"):
                cp = nearest_charge_point(zcfg, pos)                                    #calcola il punto di carica più vicino
                if not close_enough(pos, cp):
                    prev = pos 
                    new_pos = step(prev, cp, speed)                                     #calcola la nuova posizione 
                    km = haversine_km(prev, new_pos)
                    new_battery = max(0.0, new_battery - km*BATTERY_PER_KM)             #calcola la batteria per fare quello step
                else:
                    at_charge   = True
                    new_battery = min(100.0, new_battery + CHARGE_PER_TICK)             #calcola la batteria aggiunta nel tick

            # --- SCRITTURA SICURA: CAS + retry ---
            # NON tocca status/current_delivery/type/speed
            latest = None
            for _ in range(10):
                old = await kv_get(c, f"drone:{did}") or {}                             #prende il documento del vecchio drone 
                new = dict(old)                                                         #fa una copia 
                new.update({"pos": new_pos, "battery": new_battery, "at_charge": at_charge}) #la modifica
                if await kv_cas(c, f"drone:{did}", old, new):                           #prova a fare il cas (se riesce esce dal for se no ci riprova)
                    latest = new
                    break
                                                                                        # CAS fallita -> qualcun altro ha scritto: riprova
                await asyncio.sleep(0)                                                  # yield
            if latest is None:
                
                latest = await kv_get(c, f"drone:{did}") or {}                          #fa una read finale per avere memorizzato uno stato coerente da pubblicare se il cas è fallito

                                                                                        # costruisce l'evento di telemetria attuale 
            evt = {
                "type": "drone_update",
                "drone_id": latest.get("id", did),
                "pos": latest.get("pos"),
                "battery": latest.get("battery"),
                "status": latest.get("status"),
                "current_delivery": latest.get("current_delivery"),
                "at_charge": latest.get("at_charge", False),
            }
            try:
                evt_q.put_nowait(evt)                                   #infila nella coda locale l'evento 
            except asyncio.QueueFull:
                                                                        # scarta il più vecchio e inserisci l’ultimo (il più “fresco”)
                try:
                    _ = evt_q.get_nowait()
                    evt_q.task_done()
                except Exception:
                    pass
                try:
                    evt_q.put_nowait(evt)
                except Exception:
                    pass

        except Exception as e:
            print(f"[drone {did}] WARN tick:", type(e).__name__, e)

        await asyncio.sleep(TICK_SEC)

# ===== main =====
async def main():
    """
    Entry-point asincrono del simulatore di droni.

    - Recupera la zones_config.
    - Registra/aggiorna il pool dei droni nel KV (indice + documenti).
    - Avvia un task publisher per gli eventi.
    - Avvia un task run_one(...) per ciascun drone e li attende.

    Returns:
      None (termina solo su eccezione/stop).
    """
    print("[drone_sim] starting…")

    async with httpx.AsyncClient(base_url=KV_URL, timeout=HTTP_TIMEOUT) as client:          #Crea un client HTTP asincrono verso il KV
                                                                                            # bootstrap
        zcfg = await get_zcfg(client)                                                       #carica la config delle zone,
        await register_pool(client, zcfg, n_total=int(os.getenv("DRONE_POOL_MAX","20")))    #registra/inizializza l’intera flotta nel KV (n droni),
        idx = await kv_get(client, "drones_index") or []                                    #legge la lista degli ID droni.

                                                                                            # coda eventi + publisher dedicato
        evt_q = asyncio.Queue(maxsize=EVENT_QUEUE_MAX)                                      #Crea la coda interna degli eventi
        pub_task = asyncio.create_task(publisher_task(evt_q))                               #avvia il publisher dedicato che spedirà gli eventi su RabbitMQ

                                                                                            # una task per ogni drone
        tasks = [asyncio.create_task(run_one(d, client, zcfg, evt_q)) for d in idx]         #Avvia una coroutine per ciascun drone
        try:
            await asyncio.gather(*tasks)
        finally:
            pub_task.cancel()
            try:
                await pub_task
            except Exception:
                pass

if __name__ == "__main__":
    asyncio.run(main())                                                                     #esegue l'async main 
