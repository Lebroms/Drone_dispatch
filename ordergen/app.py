import os, asyncio, random, time
import httpx
import uuid


GATEWAY_URL = os.getenv("LB_URL", "http://gateway:8000")   #url del gateway
TOTAL       = int(os.getenv("TOTAL", "0"))  # loop infinito (numero totale di richieste)

# Profilo traffico a cicli 

#parametri richieste per sec
LOW_RPS     = float(os.getenv("LOW_RPS",  "0.5"))   # rich/sec in fase bassa
PEAK_RPS    = float(os.getenv("PEAK_RPS", "2.0"))   # rich/sec in fase di picco
#durata periodi
LOW1_SEC    = int(os.getenv("LOW1_SEC",  "30"))     # durata fase bassa iniziale (s)
PEAK_SEC    = int(os.getenv("PEAK_SEC",  "10"))     # durata fase picco (s)
LOW2_SEC    = int(os.getenv("LOW2_SEC",  "20"))     # durata seconda fase bassa (s)
SILENT_SEC  = int(os.getenv("SILENT_SEC","60"))     # durata pausa senza richieste (s)

CYCLE_SEC   = LOW1_SEC + PEAK_SEC + LOW2_SEC + SILENT_SEC   # durata di un ciclo

# Peso pacco (definiscono i limiti in cui generare)
WEIGHT_MIN  = float(os.getenv("WEIGHT_MIN", "0.2"))
WEIGHT_MAX  = float(os.getenv("WEIGHT_MAX", "10.0"))

def current_rps(t_elapsed: float) -> float:
    """
    Calcola il rate di richieste al secondo (RPS) da generare in funzione
    del tempo trascorso, seguendo un ciclo a 4 fasi (LOW1 → PEAK → LOW2 → SILENT).

    Args:
        t_elapsed (float): Secondi trascorsi dall'inizio della simulazione.

    Returns:
        float: Valore del rate (richieste/sec) da usare nella fase corrente:
            - LOW_RPS  → fase bassa iniziale
            - PEAK_RPS → fase di picco
            - LOW_RPS  → seconda fase bassa
            - 0.0      → fase silenziosa (nessuna richiesta)
    """
    phase = t_elapsed % CYCLE_SEC  #usa il modulo (resto della divisione intera) per capire in che fase siamo 
    if phase < LOW1_SEC:
        return LOW_RPS
    elif phase < LOW1_SEC + PEAK_SEC:
        return PEAK_RPS
    elif phase < LOW1_SEC + PEAK_SEC + LOW2_SEC:
        return LOW_RPS
    else:
        return 0.0
    
async def wait_gateway(client: httpx.AsyncClient, timeout=60):
    """
    Attende che il servizio Gateway sia pronto, verificando gli endpoint
    di health-check (/health) e configurazione zone (/zones).

    La funzione esegue ripetuti tentativi finché entrambi gli endpoint
    non rispondono con HTTP 200, oppure finché non scade il timeout.

    Args:
        client (httpx.AsyncClient): Client HTTP asincrono già aperto,
            usato per effettuare le richieste al gateway.
        timeout (int, optional): Numero massimo di secondi da attendere
            prima di considerare il gateway non raggiungibile.
            Default = 60.

    Returns:
        None: La funzione termina con successo (senza eccezioni) non appena
        il gateway risponde positivamente sia a /health che a /zones.
    """
    deadline = time.time() + timeout  #calcola il tempo limite
    while time.time() < deadline: #loop finché non scade il timeout.
        try:
            r = await client.get(f"{GATEWAY_URL}/health", timeout=2.0) #chiama l’endpoint /health del gateway
            if r.status_code == 200:
                z = await client.get(f"{GATEWAY_URL}/zones", timeout=2.0) #chiama anche l'endpoint /zones
                if z.status_code == 200:
                    return
        except Exception:
            pass
        await asyncio.sleep(1)
    raise RuntimeError("Gateway/zones non raggiungibili entro il timeout")

def rand_point_in_bounds(b: dict) -> dict:
    """
    Genera un punto geografico casuale all'interno dei limiti (bounds) forniti.

    Args:
        b (dict): Dizionario che rappresenta i limiti della zona, con chiavi:
            - "lat_min" (float): Latitudine minima consentita.
            - "lat_max" (float): Latitudine massima consentita.
            - "lon_min" (float): Longitudine minima consentita.
            - "lon_max" (float): Longitudine massima consentita.

    Returns:
        dict: Punto casuale all'interno dei bounds, con struttura:
            {
                "lat": float,  # latitudine generata casualmente
                "lon": float   # longitudine generata casualmente
            }
    """
    return {
        "lat": random.uniform(b["lat_min"], b["lat_max"]),
        "lon": random.uniform(b["lon_min"], b["lon_max"]),
    }

async def send_one(client: httpx.AsyncClient, zones: list[dict]):
    """
    Crea e invia una nuova richiesta di consegna al gateway (/deliveries).

    La funzione sceglie casualmente una zona di origine e una di destinazione
    (che possono coincidere), genera due coordinate casuali all’interno dei 
    rispettivi bounds e seleziona un peso casuale. Invia quindi una POST al 
    gateway con questi dati, includendo un header `Idempotency-Key` unico 
    (UUID) per garantire idempotenza in caso di retry.

    Args:
        client (httpx.AsyncClient): 
            Client HTTP asincrono già inizializzato e collegato al gateway.
        zones (list[dict]): 
            Lista di zone disponibili, ciascuna con almeno:
                - "name" (str): Nome della zona.
                - "bounds" (dict): Rettangolo geografico con chiavi 
                  "lat_min", "lat_max", "lon_min", "lon_max".

    Returns:
        None.  
        Stampa a console un log dell’ordine inviato:
          - Se `201 Created`: mostra ID ordine, peso, zona origine/destinazione.
          - Altrimenti: logga il codice di errore HTTP e il testo della risposta.

    """
    zo = random.choice(zones)
    zd = random.choice(zones)  # può coincidere o essere diversa
    origin = rand_point_in_bounds(zo["bounds"])  #estrae un punto sia di origine che di destination tra le bound delle zone scelte 
    dest   = rand_point_in_bounds(zd["bounds"])
    weight = round(random.uniform(WEIGHT_MIN, WEIGHT_MAX), 2)  #genera un peso randomico nel range 

    resp = await client.post(
        f"{GATEWAY_URL}/deliveries",
        json={"origin": origin, "destination": dest, "weight": weight},
        headers={"Idempotency-Key": str(uuid.uuid4())},#sto generando l'idempotency key
    )  #fa la POST all'endpoint del gateway mettendo body json con origin destinatione  gateway e come headers un idempotency key unico cosi non cisono duplicati in caso di retry
    if resp.status_code == 201:
        rid = resp.json().get("id")  
        print(f"[ordergen] creato ordine id={rid} w={weight} zo={zo['name']} zd={zd['name']}")
    else:
        print(f"[ordergen] HTTP {resp.status_code}: {resp.text}")

async def main():
    """
    Entry point asincrono del generatore di ordini (order generator).

    - Attende che il gateway e le zone siano pronti (usando `wait_gateway`).
    - Recupera la configurazione delle zone una sola volta dal gateway.
    - Calcola il tempo trascorso dall’avvio e, in base al profilo a 4 fasi
      (LOW1 → PEAK → LOW2 → SILENT), determina il rate corrente (RPS).
    - Genera i tempi di inter-arrivo con distribuzione esponenziale 
      (`Exp(λ = rps)`) per simulare traffico poissoniano.
    - Invia richieste al gateway tramite `send_one` durante le fasi attive,
      evitando invii nella fase di silenzio.
    - Termina se è stato raggiunto il numero massimo di richieste (`TOTAL`),
      altrimenti continua indefinitamente.

    Args:
        None.  
        (Tutti i parametri di configurazione sono forniti via variabili 
        d’ambiente: `TOTAL`, `LOW_RPS`, `PEAK_RPS`, `LOW1_SEC`, ecc.).

    Returns:
        None.  
        Stampa log a console per:
          - avvio e readiness del gateway,
          - invii di consegne con ID, peso e zone,
          - eventuali errori di rete,
          - completamento quando `TOTAL` è raggiunto.
    """
    sent = 0
    
    async with httpx.AsyncClient(timeout=5.0) as client: #Apre un client HTTP asincrono
        await wait_gateway(client)  #chiama la funzione per attendere che gateway e zones siano pronti
        print(f"[ordergen] gateway pronto: {GATEWAY_URL}")

        # carica zones una volta (basta per il PoC; se vuoi, potresti ricaricarle ogni X min)
        zcfg = (await client.get(f"{GATEWAY_URL}/zones")).json() #carica la config dellle zone 
        zones = zcfg["zones"] #estrae la lista zones
        start = time.monotonic()  #fa partire il contatore temporale che serve per time_elapsed e capire in che ciclo si trova 

        while True: #loop infinito
            # calcola il rate attuale in base alla fase del ciclo e genera inter-arrivo esponenziale
            rps = current_rps(time.monotonic() - start)  #Calcola RPS corrente in funzione del tempo trascorso determinando la fase
            wait = 0.0 if rps <= 0 else random.expovariate(rps)  #genera un tempo casuale in media 1/rps

            if rps > 0:   
                try:
                    await send_one(client, zones)
                    sent += 1
                except Exception as e:
                    print(f"[ordergen] errore di rete: {e}")

            if TOTAL > 0 and sent >= TOTAL:
                print("[ordergen] completato invio richieste")
                break

            await asyncio.sleep(wait)  #aspetta il tempo casuale generato in base alla fase in cui siamo

if __name__ == "__main__":
    asyncio.run(main())