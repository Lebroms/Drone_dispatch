#!/usr/bin/env bash
# ============================================
# KV TESTS — kvfront + kvstore_a/b/c (KV-only)
#   - RF=2, LWW, CAS, Read-Repair, Hinted-Handoff,
#     Distribuzione repliche, Lock, Hashing, Cache, Resilienza
#   - Dati "reali" (delivery:*, drone:*, ecc.)
# ============================================

set -Eeuo pipefail

# --- Path radice repo per importare lib.sh (usa LB/KV helpers già pronti) ---
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" #Calcola ROOT come directory del file corrente (portabile).
source "$ROOT/lib.sh" #importa funzioni helper comuni

# --- Override: useremo solo il KV (porta 9000) + backend diretti 9001/9002/9003 ---
KV_FRONT="http://localhost:9000"
KV_A="http://localhost:9001"   # kvstore_a
KV_B="http://localhost:9002"   # kvstore_b
KV_C="http://localhost:9003"   # kvstore_c

# --- CLI flags ---
#Gestione argomenti riga di comando:
#--reuse: non ricrea i container, si riattacca ai servizi già in esecuzione per velocizzare iterazioni.
#--help / -h: stampa uso e termina.
REUSE=0
if [[ "${1:-}" == "--reuse" ]]; then
  REUSE=1
elif [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  echo "Usage: $0 [--reuse]"; exit 0
fi

# ---------------------------------------------------------
# Helpers specifici per i test (accesso diretto ai backend)
# ---------------------------------------------------------

# PUT diretto a un backend (salva *esattamente* il JSON passato come value)
# ============================================================
# Funzione: backend_put_value
# Descrizione:
#   Esegue una PUT diretta su un backend KV, salvando esattamente
#   il JSON passato come valore (senza wrapper LWW).
#
# Parametri:
#   $1 = base_url (es. http://localhost:9001)
#   $2 = key (chiave da scrivere)
#   $3 = raw_json_value (valore in formato JSON grezzo)
# ============================================================
backend_put_value() { # base_url key raw_json_value
  local base="$1" key="$2" raw="$3"
  curl -sf -X PUT -H "Content-Type: application/json" \
       -d "{\"value\":${raw}}" \
       "${base}/kv/${key}" >/dev/null
}

# ============================================================
# Funzione: backend_get_value
# Descrizione:
#   Recupera direttamente un valore da un backend KV.
#
# Parametri:
#   $1 = base_url (es. http://localhost:9001)
#   $2 = key (chiave da leggere)
#
# Output:
#   Stampa su stdout il contenuto del campo .value (JSON).
#   Se la chiave non esiste (404) → stampa stringa vuota.
#   In caso di errore → ritorna comunque senza interrompere (|| true).
# ============================================================
backend_get_value() { # base_url key
  local base="$1" key="$2"
  curl -sf "${base}/kv/${key}" | ${JQ} -rc '.value // empty' || true
}

# ============================================================
# Funzione: wrap_lww
# Descrizione:
#   Wrappa un JSON grezzo in formato "Last Write Wins" (LWW).
#   Incapsula il valore in un oggetto con timestamp (_ts) corrente
#   e campo data (data).
#
# Output:
#   {"_ts": <timestamp>, "data": <raw_json>}
# ============================================================
wrap_lww() { # raw_json_data su stdin -> stampa {"_ts":..., "data":<raw>}
  local raw
  raw="$(cat -)"                           || { echo "wrap_lww: no stdin" >&2; return 1; }
  [[ -n "$raw" ]]                          || { echo "wrap_lww: empty stdin" >&2; return 1; }
  echo "$raw" | ${JQ} -e . >/dev/null 2>&1 || { echo "wrap_lww: invalid JSON on stdin" >&2; return 1; }

  python3 - <<'PY'
import sys, time, json
raw=sys.stdin.read().strip()
ts=time.time()
print(json.dumps({"_ts":ts,"data":json.loads(raw)}))
PY
}

# ============================================================
# Funzione: wrap_lww_with_ts
# Descrizione:
#   Wrappa un JSON grezzo in formato LWW con timestamp artificiale.
#   Utile nei test per simulare scritture "vecchie".
#
# Parametri:
#   $1 = raw_json_data (stringa JSON valida)
#   $2 = ts (numero, timestamp manuale)
#
# Output:
#   {"_ts": <ts>, "data": <raw_json>}
# ============================================================
wrap_lww_with_ts() { # raw_json_data ts -> stampa {"_ts":ts,"data":<raw>}
  local raw="$1" ts="$2"
  [[ -n "${raw:-}" && -n "${ts:-}" ]]     || { echo "wrap_lww_with_ts: need RAW_JSON and TS" >&2; return 1; }
  echo "$raw" | ${JQ} -e . >/dev/null 2>&1 || { echo "wrap_lww_with_ts: RAW_JSON not valid" >&2; return 1; }
  [[ "$ts" =~ ^[0-9]+(\.[0-9]+)?$ ]]       || { echo "wrap_lww_with_ts: TS must be numeric" >&2; return 1; }

  python3 - "$raw" "$ts" <<'PY'
import sys, json
raw=sys.argv[1]; ts=float(sys.argv[2])
print(json.dumps({"_ts":ts,"data":json.loads(raw)}))
PY
}


# ============================================================
# Funzione: stop_backend_b
# Descrizione:
#   Ferma il container Docker kvstore_b.
#   Usato nei test per simulare un backend "down" (per hinted-handoff).
# ============================================================
stop_backend_b(){ docker compose stop kvstore_b >/dev/null; }

# ============================================================
# Funzione: start_backend_b
# Descrizione:
#   Riavvia il container Docker kvstore_b precedentemente fermato.
#   Usato nei test per verificare la consegna degli hint.
# ============================================================
start_backend_b(){ docker compose start kvstore_b >/dev/null; }

# ============================================================
# Funzione: compose_up_kv_only
# Descrizione:
#   Ricrea l’ambiente Docker solo con i servizi KV
#   (kvstore_a, kvstore_b, kvstore_c e kvfront).
#
# Flusso:
#   1) Pulisce eventuali container/volumi precedenti
#      (docker compose down -v).
#   2) Avvia SOLO i container KV (senza dispatcher, gateway ecc.).
#   3) Attende che tutti i servizi KV siano pronti,
#      verificando la rotta /health su ciascun servizio.
#
# Output:
#   Stampa log (section, green) per indicare lo stato.
# ============================================================
compose_up_kv_only(){
  section "Pulizia ambiente precedente"
  docker compose down -v --remove-orphans || true

  section "Avvio SOLO KV (kvstore_a/b/c + kvfront)"
  docker compose up -d kvstore_a kvstore_b kvstore_c kvfront

  section "Attendo readiness dei KV"
  wait_health "${KV_FRONT}/health"      "kvfront"
  wait_health "${KV_A}/health"          "kvstore_a"
  wait_health "${KV_B}/health"          "kvstore_b"
  wait_health "${KV_C}/health"          "kvstore_c"
  green "KV pronti ✔"
}



# ============================================================
# Funzione: attach_kv_only
# Descrizione:
#   Non ricrea l’ambiente da zero, ma si aggancia a un cluster
#   KV già in esecuzione.
#
# Flusso:
#   1) Verifica la disponibilità dei servizi kvfront, kvstore_a/b/c
#      chiamando /health su ciascun endpoint.
#   2) Se tutti rispondono → conferma che i KV sono raggiungibili.
#
# Output:
#   Stampa log (section, green).
# ============================================================
attach_kv_only(){
  section "Verifico che i KV siano su (--reuse)"
  wait_health "${KV_FRONT}/health"      "kvfront"
  wait_health "${KV_A}/health"          "kvstore_a"
  wait_health "${KV_B}/health"          "kvstore_b"
  wait_health "${KV_C}/health"          "kvstore_c"
  green "KV raggiungibili ✔"
}



# ============================================================
# TEST 1 — PUT/GET + LWW (write vecchia non sovrascrive la nuova)
# Scopo:
#   Verifica che il front (kvfront) applichi correttamente il Last-Write-Wins:
#   se una replica contiene una versione con timestamp più vecchio, la GET via
#   front deve restituire la versione più recente (e non venire "inquinate").
#
# Passi:
#   1) Scrive una delivery "nuova" via front (front wrappa con _ts=now).
#   2) Bypassa il front e scrive su kvstore_a una versione "vecchia" (ts=1)
#      con un campo extra "note":"old".
#   3) Esegue GET via front e verifica che:
#        - lo status sia coerente
#        - NON compaia "note":"old" (segno che NON ha vinto la replica vecchia)
# ============================================================
test_basic_put_get_lww(){
  section "TEST 1: PUT/GET + LWW (write vecchia non sovrascrive la nuova)"
  local key="delivery:test-basic-1"

  # Valore realistico delivery
  local new_data='{"id":"test-basic-1","status":"pending","origin":{"lat":41.90,"lon":12.50},"destination":{"lat":41.85,"lon":12.55},"weight":3.2}'
  # Scriviamo via front (front incapsula con _ts e data)
  kv_put_obj "$key" "$new_data"

  # Simuliamo una replica vecchia: scriviamo DIRETTAMENTE su kvstore_a una versione "ancora pending" con TS PIÙ VECCHIO
  local old_data='{"id":"test-basic-1","status":"pending","origin":{"lat":41.90,"lon":12.50},"destination":{"lat":41.85,"lon":12.55},"weight":3.2,"note":"old"}'
  local wrapped_old
  wrapped_old="$(wrap_lww_with_ts "$old_data" "1")" # ts=1 => super vecchio
  backend_put_value "${KV_A}" "$key" "$wrapped_old"

  # Lettura via front: deve vincere la scrittura più recente (quella fatta via front)
  local got; got="$(kv_get "$key")"
  log "front GET -> $got"
  echo "$got" | grep -q '"status":"pending"' || { red "FAIL: status mismatch"; return 1; }
  echo "$got" | grep -vq '"note":"old"'       || { red "FAIL: old note leaked"; return 1; }
  green "PASS: LWW ok (vecchio TS non sovrascrive)"
}

# ============================================================
# TEST 2 — CAS: success, fail, race
# Scopo:
#   Verificare la semantica CAS del KV:
#     2a) Successo quando 'old' coincide con lo stato corrente.
#     2b) Fallimento quando 'old' è sbagliato.
#     2c) In condizioni di race (2 CAS concorrenti dallo stesso 'old'),
#         solo UNA deve vincere (l’altra fallisce).
# ============================================================
test_cas_success_fail_race(){
  section "TEST 2: CAS (ok, ko, race)"

  local key="drone:test-cas-1"
  local initial='{"id":"drone-x","status":"idle","battery":80,"pos":{"lat":41.91,"lon":12.50}}'
  kv_put_obj "$key" "$initial"

  nap 10

  # 2a) CAS con old corretto → deve riuscire
  local cur="$(kv_get "$key")"
  local new='{"id":"drone-x","status":"charging","battery":25,"pos":{"lat":41.91,"lon":12.50}}'
  local ok; ok="$(kv_cas "$key" "$cur" "$new")"
  [[ "$ok" == "true" ]] || { red "FAIL: CAS (success)"; return 1; }
  green "CAS success ✔"
  nap 10

  # 2b) CAS con old sbagliato → deve fallire
  local wrong_old='{"id":"drone-x","status":"idle","battery":80,"pos":{"lat":41.91,"lon":12.50}}'
  local new2='{"id":"drone-x","status":"idle","battery":90,"pos":{"lat":41.91,"lon":12.50}}'
  ok="$(kv_cas "$key" "$wrong_old" "$new2")"
  [[ "$ok" == "false" ]] || { red "FAIL: CAS doveva fallire"; return 1; }
  green "CAS fail ✔"

  nap 10

  # 2c) Race: due CAS concorrenti partendo dallo stesso stato → una sola vince
  #     Reset stato
  kv_put_obj "$key" "$initial"
  local base="$(kv_get "$key")"
  local updA='{"id":"drone-x","status":"busy","battery":79,"pos":{"lat":41.92,"lon":12.51},"current_delivery":"D-A"}'
  local updB='{"id":"drone-x","status":"busy","battery":79,"pos":{"lat":41.92,"lon":12.51},"current_delivery":"D-B"}'

  # lanciamo due CAS in parallelo
  ( kv_cas "$key" "$base" "$updA" > /tmp/casA.out ) &
  ( kv_cas "$key" "$base" "$updB" > /tmp/casB.out ) &
  wait

  local ra rb; ra="$(cat /tmp/casA.out)"; rb="$(cat /tmp/casB.out)"
  log "CAS A -> $ra ; CAS B -> $rb"
  if [[ "$ra" == "true" && "$rb" == "false" ]] || [[ "$ra" == "false" && "$rb" == "true" ]]; then
    green "Race OK: una sola CAS vince ✔"
  else
    red "FAIL: race non deterministica (atteso 1 true / 1 false)"; return 1
  fi
}

# ============================================================
# TEST 3 — Read-Repair (C2)
# Scopo:
#   Verificare che una GET via kvfront:
#     - Applichi LWW scegliendo il valore con timestamp più recente.
#     - Esegua read-repair sulle repliche che hanno risposto ma sono stantie,
#       riallineandole con il valore "migliore".
#
# Passi:
#   1) Scrive un valore "fresco" via front (TS recente).
#   2) Scrive direttamente su kvstore_b un valore "vecchio" (TS=2) con "note":"stale".
#   3) GET via front: deve restituire il valore nuovo e innescare il read-repair.
#   4) Verifica che kvstore_b sia stato aggiornato (nota "stale" sparita).
# ============================================================
test_read_repair(){
  section "TEST 3: Read-Repair (C2)"

  local key="delivery:test-read-repair-1"
  local fresh='{"id":"test-read-repair-1","status":"pending","origin":{"lat":41.97,"lon":12.38},"destination":{"lat":41.85,"lon":12.54},"weight":5.0}'
  # Scrivo via front (wrap con TS nuovo)
  kv_put_obj "$key" "$fresh"
  nap 10

  # Metto B stantio: valore con TS vecchio
  local stale='{"id":"test-read-repair-1","status":"pending","weight":5.0,"note":"stale"}'
  local wrapped_stale; wrapped_stale="$(wrap_lww_with_ts "$stale" "2")"  # TS=2, comunque < now
  backend_put_value "${KV_B}" "$key" "$wrapped_stale"
  nap 10

  # GET via front (sceglierà il più nuovo e poi riparerà le repliche vecchie che hanno risposto)
  local got="$(kv_get "$key")"
  log "front GET -> $got"
  echo "$got" | grep -vq '"note":"stale"' || { red "FAIL: front non ha scelto il più nuovo"; return 1; }
  nap 10

  # Dai tempo al read-repair (sincrono nel tuo front per repliche che hanno risposto e sono vecchie)
  sleep 1
  # Ora anche B dovrebbe essere allineato (se ha risposto al GET). Verifichiamo che NON contenga più la nota 'stale'.
  local bnow="$(backend_get_value "${KV_B}" "$key")"
  log "backend B after repair -> $bnow"
  echo "$bnow" | grep -vq '"note":"stale"' || { red "FAIL: B non riparato"; return 1; }
  green "PASS: Read-Repair ha aggiornato la replica stantia ✔"

  nap 10
}

# ============================================================
# TEST 4 — Hinted-Handoff (C3)
# Scopo:
#   Verificare che, se una replica è down durante una PUT via front:
#     - la write venga accettata comunque (sloppy quorum: W>=1),
#     - il front accodi un "hint" per la replica giù,
#     - al riavvio della replica, il flusher periodico consegni l'hint,
#       riallineando la replica.
#
# Passi:
#   1) Ferma kvstore_b (simula nodo down).
#   2) PUT via front della chiave/valore.
#   3) Riavvia kvstore_b e attendi il flush degli hint.
#   4) Verifica che kvstore_b ora contenga il valore scritto.
# ============================================================
test_hinted_handoff(){
  section "TEST 4: Hinted-Handoff (C3)"

  local key="drone:test-hinted-1"
  local data='{"id":"drone-hh","status":"idle","battery":65,"pos":{"lat":41.80,"lon":12.60}}'

  nap 10

  # Spengo B per simulare replica down nel ring
  stop_backend_b
  yellow "kvstore_b stoppato (simulazione replica down)"
  sleep 1

  nap 15

  # PUT via front → deve andare comunque OK (W>=1) e accodare un hint per B
  kv_put_obj "$key" "$data"
  green "Front ha accettato la write (sloppy quorum)"

  nap 10

  # Riaccendo B e attendo il flusher hint
  start_backend_b
  wait_health "${KV_B}/health" "kvstore_b (riavviato)"
  log "attendo flush hint…"; sleep 3

  nap 10

  # Ora B dovrebbe avere il valore (hint consegnato)
  local bval="$(backend_get_value "${KV_B}" "$key")"
  log "backend B value -> $bval"
  echo "$bval" | grep -q '"id":"drone-hh"' || { red "FAIL: hint non consegnato a B"; return 1; }
  green "PASS: hinted-handoff consegnato a B ✔"
}

# ============================================================
# TEST 5 — Distribuzione RF=2
# Scopo:
#   Verificare che, con replication factor RF=2, una write via kvfront
#   finisca esattamente su DUE backend (primario + 1 replica).
#
# Passi:
#   1) Scrive una chiave via front.
#   2) Legge direttamente i tre backend per verificare su quali è presente.
#   3) Conta le repliche che ce l’hanno: deve essere esattamente 2.
# ============================================================
test_rf_distribution(){
  section "TEST 5: Distribuzione RF=2"

  local key="delivery:test-rf-2"
  local data='{"id":"test-rf-2","status":"pending","origin":{"lat":41.90,"lon":12.50},"destination":{"lat":41.84,"lon":12.53},"weight":2.4}'
  kv_put_obj "$key" "$data"

  # Lettura diretta sui tre backend
  local a b c
  a="$(backend_get_value "${KV_A}" "$key")"
  b="$(backend_get_value "${KV_B}" "$key")"
  c="$(backend_get_value "${KV_C}" "$key")"

  local have=0
  [[ -n "$a" ]] && have=$((have+1))
  [[ -n "$b" ]] && have=$((have+1))
  [[ -n "$c" ]] && have=$((have+1))

  log "repliche con la chiave: $have (atteso: 2)"
  [[ $have -eq 2 ]] || { red "FAIL: atteso 2 repliche con la chiave"; return 1; }
  green "PASS: RF=2 rispettato ✔"
}

# ============================================================
# TEST 6 — Locks (acquire/release/TTL base)
# Scopo:
#   Verificare la semantica base dei lock via kvfront:
#     - acquire quando libero,
#     - acquire fallisce se già acquisito,
#     - release libera,
#     - acquire torna a riuscire dopo il release.
# ============================================================
test_locks(){
  section "TEST 6: Locks (acquire/release/TTL)"

  local lkey="delivery:test-lock-1"

  # acquire → OK
  local ok; ok="$(curl -sf -X POST "${KV_FRONT}/lock/acquire/${lkey}" | ${JQ} -r .ok)"
  [[ "$ok" == "true" ]] || { red "FAIL: acquire #1"; return 1; }

  # acquire di nuovo → KO
  ok="$(curl -sf -X POST "${KV_FRONT}/lock/acquire/${lkey}" | ${JQ} -r .ok)"
  [[ "$ok" == "false" ]] || { red "FAIL: acquire #2 doveva fallire"; return 1; }

  # release → OK
  curl -sf -X POST "${KV_FRONT}/lock/release/${lkey}" >/dev/null

  # acquire di nuovo → OK
  ok="$(curl -sf -X POST "${KV_FRONT}/lock/acquire/${lkey}" | ${JQ} -r .ok)"
  [[ "$ok" == "true" ]] || { red "FAIL: acquire #3"; return 1; }
  green "PASS: lock acquire/release ok ✔"
}



# ============================================================
# TEST 6b — Locks TTL (scadenza automatica)
# Scopo:
#   Verificare che un lock acquisito con TTL:
#     - impedisca nuove acquire finché non scade,
#     - dopo la scadenza sia nuovamente acquisibile,
#     - release sia idempotente.
# ============================================================

test_locks_ttl(){
  section "TEST 6b: Locks TTL (expire automatico)"

  local lkey="delivery:test-lock-ttl"

  # 1) Acquire con TTL molto corto (2s)
  local ok
  ok="$(curl -sf -X POST "${KV_FRONT}/lock/acquire/${lkey}?ttl_sec=2" | ${JQ} -r .ok)"
  [[ "$ok" == "true" ]] || { red "FAIL: acquire iniziale con TTL"; return 1; }
  log "acquire #1 ok (ttl=2s)"

  # 2) Subito dopo, un secondo acquire deve fallire (lock ancora valido)
  ok="$(curl -sf -X POST "${KV_FRONT}/lock/acquire/${lkey}?ttl_sec=2" | ${JQ} -r .ok)"
  [[ "$ok" == "false" ]] || { red "FAIL: acquire #2 doveva fallire (lock ancora valido)"; return 1; }
  log "acquire #2 correttamente fallito"

  # 3) Attendo oltre il TTL e riprovo (deve riuscire)
  sleep 3
  ok="$(curl -sf -X POST "${KV_FRONT}/lock/acquire/${lkey}?ttl_sec=2" | ${JQ} -r .ok)"
  [[ "$ok" == "true" ]] || { red "FAIL: acquire dopo expiry doveva riuscire"; return 1; }
  log "acquire dopo expiry ok"

  # 4) Release (idempotenza: anche se era scaduto o ri-acquisito, release deve andare)
  curl -sf -X POST "${KV_FRONT}/lock/release/${lkey}" >/dev/null || { red "FAIL: release"; return 1; }

  green "PASS: TTL lock verificato ✔"
}

# ============================================================
# TEST 7 — Consistent hashing “stabile”
# Scopo:
#   Verificare che una stessa chiave venga mappata sempre sulla
#   stessa coppia di backend (RF=2), finché non cambia l’anello
#   (lista BACKENDS o RF).
#
# Passi:
#   1) Scrive una chiave fissa via front.
#   2) Legge direttamente i tre backend per capire quali 2 la ospitano.
#   3) Stampa la coppia osservata (AB, AC o BC).
#      - Ripetendo il test senza cambiare l’anello, la coppia deve restare la stessa.
# ============================================================
test_consistent_hashing_stability(){
  section "TEST 7: Consistent hashing stabile per chiave costante"

  # Usiamo una chiave stabile “drone:hash-demo-1” e verifichiamo quali backend la ospitano
  local key="drone:hash-demo-1"
  local data='{"id":"hash-demo-1","status":"inactive","battery":100}'
  kv_put_obj "$key" "$data"

  local a b c have=""
  a="$(backend_get_value "${KV_A}" "$key")"; [[ -n "$a" ]] && have+="A"
  b="$(backend_get_value "${KV_B}" "$key")"; [[ -n "$b" ]] && have+="B"
  c="$(backend_get_value "${KV_C}" "$key")"; [[ -n "$c" ]] && have+="C"

  log "repliche correnti per ${key}: $have (atteso: qualunque coppia stabile)"
  # Non assertiamo quali due in particolare (dipende dall’anello), ma stampiamo per riferimento.
  green "PASS: mappa repliche registrata: $have ✔"
}

# ============================================================
# TEST 8 — Cache LRU smoke
# Scopo:
#   Smoke test della cache LRU sul nodo kvstore.
#   - Scrive ~200 chiavi via front (replicate RF=2).
#   - Esegue ~400 GET casuali via front per “scaldare” la cache.
#   - Il test passa se non si verificano errori durante PUT/GET.
# ============================================================
test_cache_lru_smoke(){
  section "TEST 8: Cache LRU smoke"

  # Scriviamo ~200 chiavi delivery:* e poi leggiamo ripetutamente
  for i in $(seq 1 200); do
    local key="delivery:cache-$i"
    local data="{\"id\":\"cache-$i\",\"status\":\"pending\",\"weight\":$((i%10+1))}"
    kv_put_obj "$key" "$data"
  done

  # Letture random per “scaldare” la cache
  for r in $(seq 1 400); do
    local i=$(( (RANDOM % 200) + 1 ))
    local key="delivery:cache-$i"
    kv_get "$key" >/dev/null
  done

  green "PASS: letture/cache senza errori ✔"
}

# ============================================================
# TEST 9 — Resilienza: riavvio di un backend
# Scopo:
#   Verificare che, dopo il riavvio di un nodo kvstore (es. kvstore_a),
#   il dato sia ancora servibile via kvfront (grazie a RF=2 / LWW).
#
# Passi:
#   1) Scrive una chiave via front.
#   2) Riavvia kvstore_a e attende che torni healthy.
#   3) GET via front sulla chiave: deve restituire il dato corretto.
# ============================================================
test_backend_restart_resilience(){
  section "TEST 9: Resilienza al riavvio backend"

  local key="delivery:test-resilience-1"
  local data='{"id":"test-resilience-1","status":"pending","origin":{"lat":41.93,"lon":12.48},"destination":{"lat":41.86,"lon":12.52},"weight":4.0}'
  kv_put_obj "$key" "$data"

  # Riavvio kvstore_a
  docker compose restart kvstore_a >/dev/null
  wait_health "${KV_A}/health" "kvstore_a (riavviato)"

  # Lettura via front deve continuare a funzionare
  local got="$(kv_get "$key")"
  echo "$got" | grep -q '"id":"test-resilience-1"' || { red "FAIL: dato non servibile dopo riavvio"; return 1; }
  green "PASS: dato servibile dopo restart ✔"
}

# ---------------------------------------------------------
# Main
# ---------------------------------------------------------
main(){
  if (( REUSE == 0 )); then
    compose_up_kv_only
  else
    attach_kv_only  #Se si passa --reuse allo script → si attacca a un cluster KV già avviato.
  fi

  # ESECUZIONE TEST (commenta quelli che non vuoi)
  #test_basic_put_get_lww
  #test_cas_success_fail_race
  #test_read_repair
  #test_hinted_handoff
  #test_rf_distribution
  #test_locks
  #test_locks_ttl
  #test_consistent_hashing_stability
  #test_cache_lru_smoke
  test_backend_restart_resilience

  green "Tutti i test KV completati ✔"
}

main "$@"
