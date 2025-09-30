#!/usr/bin/env bash
# ============================================================
# GL TESTS — Gateway + LB (+ opzionale Ordergen) end-to-end
#   - Verifica API gateway (/health, /zones, /deliveries, /dashboard)
#   - Idempotency-Key (CAS + re-read)
#   - Indicizzazione deliveries_index
#   - Listing /deliveries (order/limit)
#   - LB passthrough, headers base e comportamento (rate limit: opzionale)
#   - Ordergen: creazione ordini e (opz.) andamento traffico
# Requisiti: docker compose up dei servizi base, lib.sh per helpers
# ============================================================

set -Eeuo pipefail

# --- Path root repo e helpers condivisi (LB/KV/zone/drone/delivery) ---
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$ROOT/lib.sh"

# Endpoints "esterni" per i test (LB = ingresso, Gateway = dietro LB)
LB_URL="http://localhost:8080"
GW_URL="http://localhost:8000"
KV_URL="http://localhost:9000"

# Flag CLI
REUSE=0
WITH_ORDERGEN=0
RR_SETUP=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --reuse) REUSE=1 ;;
    --with-ordergen) WITH_ORDERGEN=1 ;;
    --rr|--roundrobin) RR_SETUP=1 ;;
    --help|-h)
      cat <<EOF
Usage:
  $0                     Avvia stack minimo (rabbitmq, kv*, kvfront, gateway, lb) e lancia i test
  $0 --reuse             NON riavvia i servizi, esegue solo i test
  $0 --with-ordergen     Avvia anche 'ordergen_bursty' e include test sul traffico
  $0 --rr                Forza setup round-robin per il test 5c (scale gateway=3 + ordergen acceso)
EOF
      exit 0
      ;;
    *) echo "Argomento sconosciuto: $1" >&2; exit 1 ;;
  esac
  shift
done


# ------------------------------------------------------------
# Bring-up minimo per questi test (senza dispatcher/drone)
# ------------------------------------------------------------
compose_up_min(){
  section "Pulizia ambiente precedente (solo servizi GL)"
  docker compose stop dispatcher drone >/dev/null 2>&1 || true
  docker compose rm -f dispatcher drone >/dev/null 2>&1 || true

  docker compose up -d \
    rabbitmq kvstore_a kvstore_b kvstore_c kvfront gateway lb

  if (( WITH_ORDERGEN == 1 )); then
    docker compose up -d ordergen_bursty
  fi

  section "Attendo readiness"
  wait_health "${KV_URL}/health" "kvfront"
  #wait_health "${GW_URL}/health" "gateway"
  wait_health "${LB_URL}/health" "LB"
  green "Servizi pronti ✔"
}

attach_min(){
  section "Verifico servizi esistenti (--reuse)"
  wait_health "${KV_URL}/health" "kvfront"
  #wait_health "${GW_URL}/health" "gateway"
  wait_health "${LB_URL}/health" "LB"
  if (( WITH_ORDERGEN == 1 )); then
    docker compose ps ordergen_bursty >/dev/null || { red "ordergen_bursty non presente"; exit 1; }
  fi
  green "Servizi raggiungibili ✔"
}

# ------------------------------------------------------------
# Helpers specifici per questo file
# ------------------------------------------------------------

# GET via LB (ingresso pubblico)
lb_get(){ curl -sf "${LB_URL}$1"; }
# POST /deliveries via LB con Idempotency-Key opzionale
lb_post_delivery(){
  local body="$1" idem="${2:-}"
  if [[ -n "$idem" ]]; then
    curl -sf -H "Content-Type: application/json" \
         -H "Idempotency-Key: ${idem}" \
         -d "$body" "${LB_URL}/deliveries"
  else
    curl -sf -H "Content-Type: application/json" \
         -d "$body" "${LB_URL}/deliveries"
  fi
}

# Lettura diretta KV (come nei test KV) per assert su documents
kv_get_json(){ local key="$1"; curl -sf "${KV_URL}/kv/${key}" | ${JQ} -rc '.value // empty'; }

# Crea una consegna realistica (usa bounds di /zones)
mk_random_payload(){
  local b="$(lb_get /zones | ${JQ} -c '.bounds')"
  [[ -z "$b" ]] && { red "zones bounds non disponibili"; return 1; }
  python3 - "$b" <<'PY'
import sys, json, random
b=json.loads(sys.argv[1])
def rp():
  return {"lat": round(random.uniform(b["lat_min"], b["lat_max"]),6),
          "lon": round(random.uniform(b["lon_min"], b["lon_max"]),6)}
p={"origin": rp(), "destination": rp(), "weight": round(random.uniform(0.5,9.5),2)}
print(json.dumps(p))
PY
}

# ------------------------------------------------------------
# TEST 1 — Gateway /health + /zones bootstrap idempotente
#   - se manca zones_config, gateway la crea all'avvio o on-demand
#   - /zones sempre risponde con bounds coerenti e con "zones" non vuoto
# ------------------------------------------------------------
test_gateway_health_and_zones(){
  section "TEST 1: Gateway /health + /zones bootstrap"

  # /health via LB → deve rispondere OK (LB passthrough)
  local h; h="$(lb_get /health | ${JQ} -r .status)"
  [[ "$h" == "ok" ]] || { red "FAIL: LB/gateway /health"; return 1; }

  nap 10

  # Rimuovo (se c'è) zones_config per test bootstrap on-demand
  kv_put_raw "zones_config" "null" || true

  nap 10

  # /zones via LB → il gateway la ricrea e risponde coerente
  local z; z="$(lb_get /zones)"
  echo "$z" | ${JQ} -e '.bounds and (.zones|length>0) and .rows and .cols' >/dev/null \
    || { red "FAIL: /zones non coerente"; return 1; }

  green "PASS: /health ok e /zones bootstrap idempotente ✔"
}

nap 10

# ------------------------------------------------------------
# TEST 2 — Create delivery + Idempotency-Key (dedup 201→200)
#   - Primo POST con Idempotency-Key → 201 + id
#   - Secondo POST con stessa Idempotency-Key → 200 + stesso id
#   - Verifica delivery:* e deliveries_index su KV
# ------------------------------------------------------------
test_delivery_idempotency(){
  section "TEST 2: POST /deliveries + Idempotency-Key (dedup)"

  local payload idem id1 id2 s1 s2
  payload="$(mk_random_payload)"
  idem="$(uuidgen 2>/dev/null || echo $RANDOM-$RANDOM)"

  # 1° POST
  local r1; r1="$(lb_post_delivery "$payload" "$idem")" || { red "FAIL: POST #1"; return 1; }
  id1="$(echo "$r1" | ${JQ} -r .id)"
  s1="$(echo "$r1" | ${JQ} -r .status)"
  [[ -n "$id1" && "$s1" == "pending" ]] || { red "FAIL: risposta #1 inattesa"; return 1; }

  # 2° POST con stessa key → 200
  local r2; r2="$(curl -s -w '\n%{http_code}\n' -H "Content-Type: application/json" \
               -H "Idempotency-Key: ${idem}" -d "$payload" "${LB_URL}/deliveries")"
  # split body+code
  local body2 code2; body2="$(echo "$r2" | head -n1)"; code2="$(echo "$r2" | tail -n1)"
  id2="$(echo "$body2" | ${JQ} -r .id)"
  s2="$(echo "$body2" | ${JQ} -r .status)"
  [[ "$code2" == "200" && "$id2" == "$id1" && "$s2" == "pending" ]] \
    || { red "FAIL: dedup/idempotency non rispettata"; return 1; }

  # assert su KV: documento delivery presente
  local ddoc; ddoc="$(kv_get_json "delivery:${id1}")"
  echo "$ddoc" | grep -q "\"id\":\"$id1\"" || { red "FAIL: delivery mancante su KV"; return 1; }

  # assert su deliveries_index: id presente una sola volta
  local idx; idx="$(kv_get_json "deliveries_index")"
  local count; count="$(echo "$idx" | ${JQ} -r --arg id "$id1" '[.[] | select(.==$id)] | length')"
  echo "$count"
  [[ "$count" == "1" ]] || { red "FAIL: deliveries_index contiene duplicati ($count)"; return 1; }

  green "PASS: Idempotency-Key dedup (201→200) e KV coerente ✔"
}

# ------------------------------------------------------------
# TEST 3 — Listing /deliveries (ordinamento e limit)
#   - Crea ~25 ordini
#   - GET /deliveries?limit=10 → count=10 e ordinati per timestamp desc
# ------------------------------------------------------------
test_deliveries_listing(){
  section "TEST 3: /deliveries listing (order+limit)"

  # Creo 25 ordini veloci
  for i in $(seq 1 25); do
    lb_post_delivery "$(mk_random_payload)" >/dev/null
  done
  nap 1

  local lst; lst="$(lb_get "/deliveries?limit=10")"
  local cnt; cnt="$(echo "$lst" | ${JQ} -r .count)"
  [[ "$cnt" == "10" ]] || { red "FAIL: count diverso da 10 ($cnt)"; return 1; }

  # Verifica ordine decrescente su timestamp
  local ts; ts="$(echo "$lst" | ${JQ} -r '.items[].timestamp')"
  local prev="INF"
  while IFS= read -r t; do
    if [[ "$prev" != "INF" ]]; then
      awk "BEGIN{exit !($prev >= $t)}" || { red "FAIL: non ordinato desc (prev=$prev cur=$t)"; return 1; }
    fi
    prev="$t"
  done <<< "$ts"

  green "PASS: listing limit=10 e ordine per timestamp desc ✔"
}

# ------------------------------------------------------------
# TEST 4 — /dashboard static (best-effort)
#   - Se presente static/dashboard.html → 200
#   - Altrimenti → 404 con messaggio chiaro
#   NB: Non è vincolante alla correttezza del core; è un sanity check.
# ------------------------------------------------------------
test_dashboard_static(){
  section "TEST 4: /dashboard static"
  local r; r="$(curl -s -o /dev/null -w '%{http_code}' "${LB_URL}/dashboard" || echo ERR)"
  if [[ "$r" == "200" ]]; then
    green "PASS: dashboard servita (200) ✔"
  elif [[ "$r" == "404" ]]; then
    green "PASS: dashboard non presente ma 404 chiaro ✔"
  else
    red "WARN: risposta inattesa o errore ($r) — non blocco, ma segnalo"
  fi
}

# ------------------------------------------------------------
# TEST 5 — LB passthrough (basics) e header Content-Type
#   - GET /zones via LB deve restituire JSON valido
#   - Verifica header Content-Type applicato dal proxy
# ------------------------------------------------------------
test_lb_passthrough(){
  section "TEST 5: LB passthrough /zones"

  # Ottengo anche headers per controllare content-type propagato
  local raw; raw="$(curl -si "${LB_URL}/zones")"
  local code ct body
  code="$(echo "$raw" | head -n1 | awk '{print $2}')"
  ct="$(echo "$raw" | tr -d '\r' | grep -i '^content-type:' | awk -F': ' '{print tolower($2)}' | head -n1)"
  body="$(echo "$raw" | sed -n '/^\r\{0,1\}$/,$p' | tail -n +2)"

  [[ "$code" == "200" ]] || { red "FAIL: LB /zones HTTP $code"; return 1; }
  echo "$body" | ${JQ} -e '.zones and (.zones|length>0)' >/dev/null \
    || { red "FAIL: LB /zones body non valido"; return 1; }
  [[ "$ct" == "application/json" || "$ct" == "application/json; charset=utf-8" ]] \
    || { yellow "WARN: content-type non canonico ($ct)"; }

  green "PASS: LB /zones passthrough ok ✔"
}

#============runnare con ./gl_tests.sh --reuse --rr ==========================================================

test_lb_round_robin_with_ordergen(){
  section "TEST 5c: Round-robin reale (gateway logs) con ordergen"

  # [NUOVO] setup forzato se richiesto da CLI (--rr)
  if (( RR_SETUP == 1 )); then
    yellow "Forzo setup RR: scale gateway=3 + ordergen_bursty ON"
    docker compose up -d --scale gateway=3 ordergen_bursty
    # piccola attesa per far partire i nuovi gateway e l'ordergen
    sleep 5
  fi

  # 1) serve >=2 gateway (usa gli ID in esecuzione)
  local gw_ids gw_count
  mapfile -t gw_ids < <(docker compose ps -q gateway 2>/dev/null | sed '/^\s*$/d')
  gw_count="${#gw_ids[@]}"
  if [[ "$gw_count" -lt 2 ]]; then
    red "FAIL: servono almeno 2 gateway in esecuzione (visti: $gw_count)"
    red "Suggerimento: docker compose up -d --scale gateway=3"
    return 1
  fi
  log "gateway running = $gw_count"

  # 2) accendi/riusa ordergen_bursty
  local started_og=0
  if [[ -z "$(docker compose ps -q ordergen_bursty 2>/dev/null)" ]]; then
    log "accendo ordergen_bursty…"
    docker compose up -d ordergen_bursty
    started_og=1
  else
    log "ordergen_bursty già attivo ✔"
  fi

  # 3) finestra di osservazione
  local window=60
  log "osservo traffico per ${window}s…"
  sleep "$window"

  # 4) conta le righe NEW delivery per ciascun gateway
  declare -A hits
  local id
  for id in "${gw_ids[@]}"; do
    local n
    n="$(docker logs --since ${window}s "$id" 2>&1 | grep -c '\[gateway\] NEW delivery id=')" || n=0
    hits["$id"]="$n"
  done

  # 5) stampa distribuzione
  echo "Distribuzione consegne per gateway:"
  local active=0 total=0 min=999999 max=0
  for id in "${gw_ids[@]}"; do
    local name; name="$(docker inspect --format '{{.Name}}' "$id" | sed 's#^/##')"
    local c="${hits[$id]:-0}"
    echo " - ${name}: $c"
    total=$((total + c))
    (( c > 0 )) && active=$((active + 1))
    (( c < min )) && min=$c
    (( c > max )) && max=$c
  done

  # 6) criteri di pass
  if [[ "$active" -lt 2 ]]; then
    red "FAIL: viste richieste su ${active} istanza(e); attese ≥2 con round-robin"
    [[ "$started_og" -eq 1 ]] && docker compose stop ordergen_bursty >/dev/null
    return 1
  fi

  green "PASS: almeno $active gateway hanno gestito consegne (tot=$total) ✔"

  # 7) check di equità (soft) se c'è un minimo di traffico
  if [[ "$total" -ge 6 && "$min" -gt 0 ]]; then
    awk "BEGIN{exit !($max <= 1.6 * $min)}" || yellow "WARN: distribuzione un po' sbilanciata (min=$min max=$max)"
  fi

  # 8) spegniamo ordergen se l’abbiamo acceso noi
  if [[ "$started_og" -eq 1 ]]; then
    log "spengo ordergen_bursty (era stato acceso dal test)…"
    docker compose stop ordergen_bursty >/dev/null
  fi
}


# ------------------------------------------------------------
# TEST 6 — LB Rate Limiter (opzionale, con vere /deliveries)
#   - Se RL_GLOBAL_RATE/BURST == 0 → skip
#   - Se configurato (>0), invia un burst di POST /deliveries
#     e si aspetta alcuni 429.
#   NOTE: per evitare “rumore”, tieni ordergen OFF per questo test.
# ------------------------------------------------------------
test_lb_rate_limit_optional(){
  section "TEST 6: LB Rate Limiter (opzionale, /deliveries reali)"

  # Leggo gli env attivi interrogando il container (best-effort)
  local rate burst
  rate="$(docker compose exec -T lb sh -lc 'echo ${RL_GLOBAL_RATE:-0}' 2>/dev/null || echo 0)"
  burst="$(docker compose exec -T lb sh -lc 'echo ${RL_GLOBAL_BURST:-0}' 2>/dev/null || echo 0)"

  if [[ "${rate:-0}" == "0" || "${burst:-0}" == "0" ]]; then
    yellow "SKIP: rate limiter globale disattivato (RL_GLOBAL_RATE/BURST=0)"
    return 0
  fi

  # Burst di POST /deliveries con Idempotency-Key univoca ogni volta
  local N=24 i=0 created=0 deduped=0 throttled=0 other=0
  while (( i < N )); do
    payload="$(mk_random_payload)"
    idem="$(uuidgen 2>/dev/null || echo $RANDOM-$RANDOM)"
    # faccio la POST e catturo body+status
    r="$(curl -s -w '\n%{http_code}\n' -H 'Content-Type: application/json' \
          -H "Idempotency-Key: ${idem}" \
          -d "$payload" "${LB_URL}/deliveries")" || r=$'\n000'
    body="$(echo "$r" | head -n1)"
    code="$(echo "$r" | tail -n1)"

    case "$code" in
      201) created=$((created+1));;
      200) deduped=$((deduped+1));;   # (può capitare raramente se riuso idem nei test)
      429) throttled=$((throttled+1));;
      *)   other=$((other+1));;
    esac
    i=$((i+1))
    # nessun sleep: vogliamo superare rate+burst
  done

  log "rate-limit deliveries: created=$created deduped=$deduped throttled=$throttled other=$other (N=$N)"
  [[ $throttled -gt 0 ]] || { red "FAIL: attesi alcuni 429 con limiter ON"; return 1; }
  green "PASS: visti 429 con Retry-After durante POST /deliveries ✔"
}



# ------------------------------------------------------------
# TEST 7 — Ordergen smoke (opzionale, se abilitato)
#   - Verifica che, con ordergen_bursty acceso, il numero di deliveries cresca in ~15s
#   - Non testiamo le 4 fasi (durate lunghe); smoke sufficiente per il PoC.
# ------------------------------------------------------------
test_ordergen_smoke_optional(){
  section "TEST 7: Ordergen smoke (opzionale)"

  if (( WITH_ORDERGEN == 0 )); then
    yellow "SKIP: ordergen non attivato (usa --with-ordergen)"
    return 0
  fi

  # Leggo la lunghezza dell'indice globale (illimitato)
  # NB: se la chiave non esiste ancora, jq fallirebbe: gestisco il caso null->0
  local c1 c2
  c1="$(kv_get_json 'deliveries_index' | ${JQ} -r 'if .==null then 0 else length end')"
  nap 45   # puoi tenere 30–60s; hai già messo SILENT=0 quindi 30-45s bastano
  c2="$(kv_get_json 'deliveries_index' | ${JQ} -r 'if .==null then 0 else length end')"

  log "deliveries_index prima=${c1} dopo=~45s -> ${c2}"
  [[ "$c2" -gt "$c1" ]] || { red "FAIL: atteso incremento consegne con ordergen attivo"; return 1; }
  green "PASS: ordergen genera richieste (indice cresce) ✔"
}


# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------
main(){
  if (( REUSE == 0 )); then
    compose_up_min
  else
    attach_min
  fi

  # === ESECUZIONE TEST (puoi commentare quelli che non vuoi) ===
  #test_gateway_health_and_zones
  #test_delivery_idempotency
  #test_deliveries_listing
  #test_dashboard_static
  #test_lb_passthrough
  test_lb_round_robin_with_ordergen
  #test_lb_rate_limit_optional
  #test_ordergen_smoke_optional

  green "Tutti i test GL completati ✔"
}

main "$@"
