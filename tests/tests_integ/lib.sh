#!/usr/bin/env bash
set -euo pipefail

LB=http://localhost:8080
KV=http://localhost:9000
JQ=${JQ:-jq}

# Endpoints diretti dei 3 backend kvstore (bypass kvfront)
KV_A=http://localhost:9001
KV_B=http://localhost:9002
KV_C=http://localhost:9003

# ==== output helpers ====
ts() { date +"%H:%M:%S"; }
log() { echo "[$(ts)] $*"; }
green(){ printf "\033[32m%s\033[0m\n" "$*"; }
yellow(){ printf "\033[33m%s\033[0m\n" "$*"; }
red(){ printf "\033[31m%s\033[0m\n" "$*"; }
section(){ echo; yellow "=== $* ==="; }

# --- HTTP helpers ---
api_get(){ curl -sf "${LB}$1"; }
api_post_delivery(){
  local body="$1"
  curl -sf -H "Content-Type: application/json" \
       -H "Idempotency-Key: $(uuidgen 2>/dev/null || echo $RANDOM-$RANDOM)" \
       -d "$body" "${LB}/deliveries"
}

# --- KV helpers ---
kv_get(){ local key="$1"; curl -sf --max-time 5 "${KV}/kv/${key}" | ${JQ} -rc '.value // empty'; }
kv_put_raw(){ local key="$1" raw="$2"; curl -sf -X PUT -H "Content-Type: application/json" -d "{\"value\":${raw}}" "${KV}/kv/${key}" >/dev/null; }
kv_put_obj(){ local key="$1" obj="$2"; kv_put_raw "$key" "$obj"; }
kv_cas(){
  local key="$1" old="$2" new="$3"
  curl -sf -X POST -H "Content-Type: application/json" \
       -d "{\"key\":\"${key}\",\"old\":${old},\"new\":${new}}" \
       "${KV}/kv/cas" | ${JQ} -r '.ok'
}





# ==== Helper "low-level" per parlare con i kvstore DIRETTI (bypass kvfront) ====

# Incapsula un valore (JSON) nello schema LWW del front: {"_ts": <float>, "data": <obj>}
wrap_lww_with_ts(){
  local data_json="$1" ts="$2"
  python3 - <<PY
import sys, json
data = json.loads("""$data_json""")
ts = float($ts)
print(json.dumps({"_ts": ts, "data": data}, separators=(",",":")))
PY
}

# PUT diretto su un backend (value deve essere già "wrapped" se vuoi forzare il _ts)
backend_put_value(){
  local base="$1" key="$2" wrapped_json="$3"
  curl -sf -X PUT -H "Content-Type: application/json" \
       -d "{\"value\":${wrapped_json}}" \
       "${base}/kv/${key}" >/dev/null
}

# GET diretto da un backend (ritorna il campo .value così come memorizzato)
backend_get_value(){
  local base="$1" key="$2"
  curl -sf --max-time 5 "${base}/kv/${key}" | ${JQ} -rc '.value // empty'
}

# Health diretto di un backend
backend_health(){ local base="$1"; curl -sf --max-time 3 "${base}/health" >/dev/null; }


# --- util ---
zones_bounds(){ api_get /zones | ${JQ} -c '.bounds'; }
rand_point(){
  local bounds="$(zones_bounds)"
  python3 - "$bounds" <<'PY'
import sys, json, random
b=json.loads(sys.argv[1])
lat=random.uniform(b["lat_min"], b["lat_max"])
lon=random.uniform(b["lon_min"], b["lon_max"])
print(json.dumps({"lat":round(lat,6),"lon":round(lon,6)}))
PY
}
list_drones(){ api_get /drones | ${JQ} -c '.items[]'; }
pick_drone_by(){ local type="$1" status="$2"; list_drones | ${JQ} -r "select(.type==\"${type}\" and .status==\"${status}\") | .id" | head -n1; }
patch_drone(){
  local did="$1" jq_patch="$2"
  local key="drone:${did}"
  local cur="$(kv_get "$key")"
  if [ -z "$cur" ] || [ "$cur" = "null" ]; then echo "ERR: drone ${did} not found" >&2; return 1; fi
  local new="$(echo "$cur" | ${JQ} -c "${jq_patch}")"
  local ok="$(kv_cas "$key" "$cur" "$new")"
  echo "$ok"
}
create_delivery(){ local origin="$1" dest="$2" weight="$3"; api_post_delivery "{\"origin\":${origin},\"destination\":${dest},\"weight\":${weight}}"; }
nap(){ local s="${1:-1}"; log "sleep ${s}s"; sleep "$s"; }

# ==== prereq & readiness ====
require_tool(){ local t="$1"; if ! command -v "$t" >/dev/null 2>&1; then echo "❌ tool mancante: $t" >&2; exit 1; fi; }
wait_health(){
  local url="$1" name="$2" retries="${3:-30}" delay="${4:-2}"
  echo "→ Attendo che $name sia pronto ($url)"
  for ((i=1; i<=retries; i++)); do
    if curl -sf --max-time 3 "$url" >/dev/null 2>&1; then echo "   $name OK"; return 0; fi
    sleep "$delay"
  done
  echo "❌ Timeout attendendo $name ($url)" >&2; exit 1
}
require_tool curl
require_tool ${JQ:-jq}
require_tool python3

# ====== CONTROL HELPERS ======
get_zone_bounds_by_name(){ local z="$1"; api_get /zones | ${JQ} -c --arg z "$z" '.zones[] | select(.name==$z) | .bounds'; }
rand_point_in_zone(){
  local z="$1"; local b="$(get_zone_bounds_by_name "$z")"; [[ -z "$b" ]] && { echo ""; return 1; }
  python3 - "$b" <<'PY'
import sys, json, random
b=json.loads(sys.argv[1])
lat=random.uniform(b["lat_min"], b["lat_max"])
lon=random.uniform(b["lon_min"], b["lon_max"])
print(json.dumps({"lat":round(lat,6),"lon":round(lon,6)}))
PY
}
set_drone_status(){ local id="$1" st="$2"; patch_drone "$id" ".status=\"$st\"" >/dev/null; }
set_drone_battery(){ local id="$1" pct="$2"; patch_drone "$id" ".battery=$pct" >/dev/null; }
set_drone_pos(){
  local id="$1" lat="$2" lon="$3" freeze="${4:-2}"
  local until="$(python3 -c "import time; print(time.time()+${freeze})")"
  patch_drone "$id" ".pos={\"lat\":$lat,\"lon\":$lon} | .freeze_until=$until" >/dev/null
}
move_drone_to_zone_charge(){
  local id="$1" zone="$2" freeze="${3:-2}"
  local cp="$(api_get /zones | ${JQ} -c --arg z "$zone" '.zones[] | select(.name==$z) | .charge')"
  [[ -z "$cp" ]] && { echo "❌ zona $zone non trovata" >&2; return 1; }
  local lat="$(echo "$cp" | ${JQ} -r .lat)"; local lon="$(echo "$cp" | ${JQ} -r .lon)"
  local jlat="$(python3 - <<PY
import random; print($lat + random.uniform(-0.00009,0.00009))
PY
)"
  local jlon="$(python3 - <<PY
import random; print($lon + random.uniform(-0.00009,0.00009))
PY
)"
  set_drone_pos "$id" "$jlat" "$jlon" "$freeze"
}
activate_only_ids(){
  local csv="$1"; IFS=',' read -r -a keep <<< "$csv"; declare -A ok; for k in "${keep[@]}"; do ok[$k]=1; done
  for dr in $(list_drones | ${JQ} -r .id); do
    local st="$(kv_get "drone:$dr" | ${JQ} -r .status)"
    if [[ -n "${ok[$dr]:-}" ]]; then [[ "$st" != "busy" ]] && set_drone_status "$dr" "idle"
    else [[ "$st" != "busy" ]] && set_drone_status "$dr" "inactive"; fi
  done
}

# ====== CONFIG PER-DRONE ======
# Chiavi: state=idle|inactive|charging|retiring | zone="zone N" | pos=LAT,LON | freeze=SEC | battery=PCT
DRONE_OVERRIDES=$(cat <<'EOF_OVR'
# Esempi:
# activate_only: drone-3
# drone-01 state=idle  zone="zone 2"   battery=80
# drone-02 state=inactive
# drone-03 pos=41.935,12.50  freeze=3  battery=45
# drone-07 state=idle  zone="zone 0"

# --- metti qui le tue regole ---
#  activate_only: drone-1, drone-4
#  drone-1 pos=41.91,12.54  freeze=3  battery=40
#  drone-4 pos=41.83,12.54  freeze=3  battery=40
EOF_OVR
)




DRONE_OVERRIDES_BACKUP=$(cat <<'EOF_OVR_BKP'
drone-1  state=inactive battery=100 
drone-2  state=inactive battery=100
drone-3  state=inactive battery=100
drone-4  state=inactive battery=100
drone-5  state=inactive battery=100
drone-6  state=inactive battery=100
drone-7  state=inactive battery=100
drone-8  state=inactive battery=100
drone-9  state=inactive battery=100
drone-10 state=inactive battery=100
drone-11 state=inactive battery=100
drone-12 state=inactive battery=100
drone-13 state=inactive battery=100
drone-14 state=inactive battery=100
drone-15 state=inactive battery=100
EOF_OVR_BKP
)

# Comodo helper: applica l’override di backup senza toccare DRONE_OVERRIDES “principale”
apply_backup_overrides(){
  local save="$DRONE_OVERRIDES"
  DRONE_OVERRIDES="$DRONE_OVERRIDES_BACKUP"
  section "Applico DRONE_OVERRIDES_BACKUP (drone-1..15 → inactive + battery 100)"
  apply_drone_overrides
  DRONE_OVERRIDES="$save"
}

apply_drone_overrides(){
  local had_activate_only=""
  # 1) activate_only (se presente)
  while IFS= read -r line; do
    line="${line%%#*}"; line="$(echo "$line" | sed 's/^[ \t]*//;s/[ \t]*$//')"
    [[ -z "$line" ]] && continue
    if [[ "$line" =~ ^activate_only: ]]; then
      local csv="${line#*:}"; csv="$(echo "$csv" | tr -d ' ')"; had_activate_only="$csv"; break
    fi
  done <<< "$DRONE_OVERRIDES"

  if [[ -n "$had_activate_only" ]]; then
    log "[overrides] activate_only: $had_activate_only"
    activate_only_ids "$had_activate_only"; nap 2
  fi

  # 2) per-drone
  while IFS= read -r line; do
    line="${line%%#*}"; line="$(echo "$line" | sed 's/^[ \t]*//;s/[ \t]*$//')"
    [[ -z "$line" ]] && continue
    [[ "$line" =~ ^activate_only: ]] && continue
    local id rest; read -r id rest <<<"$line"; [[ -z "$id" ]] && continue

    local want_state="" want_zone="" want_pos="" want_freeze="" want_batt=""

    # --- parser key=value che rispetta virgolette ---
    pairs=()
    while read -r kv; do pairs+=("$kv"); done < <(grep -oE '([a-z_]+)="[^"]*"|([a-z_]+)=[^[:space:]]+' <<< "$rest")
    for kv in "${pairs[@]}"; do
      k="${kv%%=*}"
      v="${kv#*=}"
      [[ "${v:0:1}" == '"' && "${v: -1}" == '"' ]] && v="${v:1:-1}"
      case "$k" in
        state)   want_state="$v" ;;
        zone)    want_zone="$v"  ;;
        pos)     want_pos="$v"   ;;
        freeze)  want_freeze="$v" ;;
        battery) want_batt="$v"  ;;
      esac
    done

    if [[ -n "$want_pos" ]]; then
      local lat="${want_pos%,*}" lon="${want_pos#*,}" fr="${want_freeze:-2}"
      log "[overrides] $id pos=($lat,$lon) freeze=$fr"; set_drone_pos "$id" "$lat" "$lon" "$fr"; nap 1
    elif [[ -n "$want_zone" ]]; then
      local fr="${want_freeze:-2}"
      log "[overrides] $id near charge of $want_zone freeze=$fr"; move_drone_to_zone_charge "$id" "$want_zone" "$fr"; nap 1
    fi
    if [[ -n "$want_batt" ]]; then log "[overrides] $id battery=$want_batt"; set_drone_battery "$id" "$want_batt"; nap 1; fi
    if [[ -n "$want_state" ]]; then log "[overrides] $id state=$want_state"; set_drone_status "$id" "$want_state"; nap 1; fi
  done <<< "$DRONE_OVERRIDES"
}

wait_delivery_status(){ # id, target, timeout
  local id="$1" target="$2" timeout="${3:-180}"
  local t0="$(date +%s)"
  while true; do
    st="$(api_get "/deliveries/$id" | ${JQ} -r .status)"
    log "delivery $id -> $st (want $target)"
    [[ "$st" == "$target" ]] && return 0
    [[ $(( $(date +%s)-t0 )) -ge $timeout ]] && return 1
    sleep 2
  done
}

# ===============================
# NUOVE FUNZIONI: PIANO CONSEGNE
# ===============================
# Definisci *una o più* consegne qui, in righe "delivery ..."
# Chiavi supportate (tutte opzionali tranne il peso):
#   weight=NUM
#   pickup_zone="zone N"    oppure  pickup_pos=LAT,LON
#   dest_zone="zone M"      oppure  dest_pos=LAT,LON
#   pause_before=SEC        (pausa prima di creare la consegna)
#   pause_after=SEC         (pausa dopo creazione o dopo attesa)
#   wait_status=pending|assigned|in_flight|delivered
#   wait_timeout=SEC
DELIVERY_PLAN=$(cat <<'EOF_PLAN'
# Esempi:
# delivery weight=1.5 pickup_zone="zone 2" dest_zone="zone 0" wait_status=delivered wait_timeout=240 pause_after=10
# delivery weight=2.0 pickup_pos=41.935,12.52 dest_zone="zone 3" wait_status=assigned
# delivery weight=3.2 pickup_zone="zone 1" dest_pos=41.845,12.5425 pause_before=10

# --- metti qui le tue righe ---
# 
delivery weight=1 pickup_zone="zone 3" dest_zone="zone 0"  pause_after=2
delivery weight=2.5 pickup_zone="zone 2" dest_zone="zone 1"  pause_after=2

delivery weight=5.5 pickup_zone="zone 2" dest_zone="zone 3"  pause_after=2
delivery weight=4 pickup_zone="zone 2" dest_zone="zone 2"  pause_after=2

delivery weight=9 pickup_zone="zone 1" dest_zone="zone 0"  pause_after=2


EOF_PLAN
)

# -- resolver punti --
_resolve_point_from_spec(){ # echo JSON {"lat":..,"lon":..}
  local zone="$1" pos="$2"
  if [[ -n "$pos" ]]; then
    local lat="${pos%,*}" lon="${pos#*,}"
    echo "{\"lat\":${lat},\"lon\":${lon}}"
  elif [[ -n "$zone" ]]; then
    rand_point_in_zone "$zone"
  else
    rand_point
  fi
}

# Se PLAN_CONCURRENT=1, le delivery del piano vengono create in parallelo (fire-and-forget)
apply_delivery_plan(){
  section "📦 Eseguo DELIVERY_PLAN (concurrent=${PLAN_CONCURRENT:-0})"
  local pids=()

  while IFS= read -r line; do
    line="${line%%#*}"; line="$(echo "$line" | sed 's/^[ \t]*//;s/[ \t]*$//')"
    [[ -z "$line" ]] && continue
    [[ "$line" != delivery* ]] && continue

    local rest="${line#delivery }"
    local weight="" pickup_zone="" dest_zone="" pickup_pos="" dest_pos=""
    local pause_before="0" pause_after="0" wait_status="" wait_timeout="180"

    # --- parser key=value che rispetta virgolette ---
    pairs=()
    while read -r kv; do pairs+=("$kv"); done < <(grep -oE '([a-z_]+)="[^"]*"|([a-z_]+)=[^[:space:]]+' <<< "$rest")
    for kv in "${pairs[@]}"; do
      k="${kv%%=*}"
      v="${kv#*=}"
      [[ "${v:0:1}" == '"' && "${v: -1}" == '"' ]] && v="${v:1:-1}"
      case "$k" in
        weight)        weight="$v"        ;;
        pickup_zone)   pickup_zone="$v"   ;;
        dest_zone)     dest_zone="$v"     ;;
        pickup_pos)    pickup_pos="$v"    ;;
        dest_pos)      dest_pos="$v"      ;;
        pause_before)  pause_before="$v"  ;;
        pause_after)   pause_after="$v"   ;;
        wait_status)   wait_status="$v"   ;;
        wait_timeout)  wait_timeout="$v"  ;;
      esac
    done
    [[ -z "$weight" ]] && { red "delivery senza weight: $line"; continue; }

    _one_delivery() {
      log "[plan] pause_before=${pause_before}s"; nap "$pause_before"

      local o d
      o="$(_resolve_point_from_spec "$pickup_zone" "$pickup_pos")"
      d="$(_resolve_point_from_spec "$dest_zone" "$dest_pos")"

      local pick_label dest_label
      pick_label="${pickup_zone:-${pickup_pos:-random}}"
      dest_label="${dest_zone:-${dest_pos:-random}}"
      log "[plan] Crea delivery weight=$weight pickup=${pick_label} dest=${dest_label}"

      local resp id oz dz
      resp="$(create_delivery "$o" "$d" "$weight")" || { red "create_delivery fallita"; return 1; }
      id="$(echo "$resp" | ${JQ} -r .id)"
      oz="$(echo "$resp" | ${JQ} -r .origin_zone)"
      dz="$(echo "$resp" | ${JQ} -r .destination_zone)"
      log "[plan] delivery id=$id pickup=$oz → dest=$dz"

      if [[ -n "$wait_status" ]]; then
        if wait_delivery_status "$id" "$wait_status" "$wait_timeout"; then
          green "[plan] delivery $id ha raggiunto $wait_status"
        else
          red   "[plan] delivery $id NON ha raggiunto $wait_status nei tempi"
        fi
      fi

      log "[plan] pause_after=${pause_after}s"; nap "$pause_after"
    }

    if [[ "${PLAN_CONCURRENT:-0}" == "1" ]]; then
      if [[ -n "$wait_status" ]]; then
        yellow "[plan] (concurrent) Ignoro wait_status=$wait_status per questa riga"
        wait_status=""
      fi
      _one_delivery & pids+=($!)
    else
      _one_delivery
    fi
  done <<< "$DELIVERY_PLAN"

  if [[ "${PLAN_CONCURRENT:-0}" == "1" && "${#pids[@]}" -gt 0 ]]; then
    log "[plan] Attendo ${#pids[@]} delivery lanciate in parallelo…"
    local fail=0
    for pid in "${pids[@]}"; do
      if ! wait "$pid"; then fail=$((fail+1)); fi
    done
    [[ $fail -gt 0 ]] && red "[plan] $fail job in errore" || green "[plan] tutte le delivery lanciate"
  fi
}





# Forza una delivery a 'delivered' via KV (best-effort, CAS)
force_delivery_delivered(){
  local id="$1"
  local key="delivery:${id}"
  local cur="$(kv_get "$key")"
  if [[ -z "$cur" || "$cur" == "null" ]]; then
    red "[cleanup] delivery $id non trovata su KV ($key)"
    return 1
  fi
  # timestamp now
  local now="$(python3 -c 'import time; print(time.time())')"
  # patch minimale: status, delivered_at, progress (se esiste)
  local new="$(echo "$cur" | ${JQ} -c --argjson now "$now" '
      .status="delivered"
      | .delivered_at=$now
      | (has("progress") then .progress=1 else . end)
  ')"
  local ok="$(kv_cas "$key" "$cur" "$new")"
  if [[ "$ok" == "true" ]]; then
    green "[cleanup] delivery $id → delivered (forzato)"
    return 0
  else
    red "[cleanup] delivery $id: CAS fallita (corsa con update reale?)"
    return 1
  fi
}

# Forza tutte le consegne non-delivered a delivered, ripulendo anche campi di viaggio
# e rilasciando l'eventuale drone assegnato. Best-effort con retry CAS.
force_finish_all_open_deliveries_strict(){
  local ids
  ids="$(api_get "/deliveries" | ${JQ} -r '.items[] | select(.status!="delivered") | .id')" || {
    red "[cleanup] non riesco a leggere /deliveries"; return 1; }

  [[ -z "$ids" ]] && { green "[cleanup] nessuna consegna aperta da chiudere"; return 0; }

  local total=0 ok=0 fail=0
  while IFS= read -r id; do
    [[ -z "$id" ]] && continue
    total=$((total+1))
    local dkey="delivery:${id}"

    # CAS delivery con retry
    local attempts=0 done=0 assignee=""
    while (( attempts < 6 )); do
      attempts=$((attempts+1))
      local cur new
      cur="$(kv_get "$dkey")"
      [[ -z "$cur" || "$cur" == "null" ]] && { red "[cleanup] $id: chiave non trovata"; break; }

      # salvo l'assegnatario (se presente) per rilasciare il drone dopo
      assignee="$(echo "$cur" | ${JQ} -r '.assigned_to // empty')"

      # patch robusto: delivered + wipe campi di viaggio/notifiche/ETAs, e azzera assegnazione
      new="$(echo "$cur" | ${JQ} -c '
        .status="delivered"
        | (if has("progress")       then .progress=1              else . end)
        | (if has("delivered_at")   then .delivered_at=now        else . end)
        | (if has("assigned_to")    then .assigned_to=null        else . end)
        | (if has("pickup_eta")     then .pickup_eta=null         else . end)
        | (if has("delivery_eta")   then .delivery_eta=null       else . end)
        | (if has("eta")            then .eta=null                else . end)
        | (if has("leg")            then .leg=null                else . end)
        | (if has("legs")           then .legs=[]                 else . end)
        | (if has("route")          then .route=[]                else . end)
        | (if has("path")           then .path=[]                 else . end)
        | (if has("waypoints")      then .waypoints=[]            else . end)
        | (if has("segment")        then .segment=null            else . end)
        | (if has("phase")          then .phase=null              else . end)
        | (if has("stage")          then .stage=null              else . end)
        | (if has("to_origin")      then .to_origin=null          else . end)
        | (if has("to_destination") then .to_destination=null     else . end)
      ' )" || { red "[cleanup] $id: jq patch error"; break; }

      local okcas
      okcas="$(kv_cas "$dkey" "$cur" "$new" || true)"
      if [[ "$okcas" == "true" ]]; then
        green "[cleanup] delivery $id → delivered & ripulita"
        done=1
        ok=$((ok+1))
        break
      else
        sleep 0.25
      fi
    done

    # rilascio drone se serve (best effort)
    if (( done == 1 )) && [[ -n "$assignee" && "$assignee" != "null" ]]; then
      local dkey2="drone:${assignee}"
      local dcur="$(kv_get "$dkey2")"
      if [[ -n "$dcur" && "$dcur" != "null" ]]; then
        local duntil="$(python3 -c 'import time; print(time.time()+1.5)')"
        local dnew="$(echo "$dcur" | ${JQ} -c '
          (if has("status")          then .status="idle"            else . end)
          | (if has("current_job")   then .current_job=null         else . end)
          | (if has("current_delivery") then .current_delivery=null else . end)
          | (if has("mission")       then .mission=null             else . end)
          | (if has("task")          then .task=null                else . end)
          | (if has("route")         then .route=[]                 else . end)
          | (if has("path")          then .path=[]                  else . end)
          | (if has("waypoints")     then .waypoints=[]             else . end)
          | (if has("target")        then .target=null              else . end)
          | (if has("busy_until")    then .busy_until=null          else . end)
        ' )"
        # piccolo "freeze" per evitare che il sim si muova immediatamente su roba vecchia
        dnew="$(echo "$dnew" | ${JQ} -c --argjson u "$duntil" '(if has("freeze_until") then .freeze_until=$u else . end)')"
        local okd; okd="$(kv_cas "$dkey2" "$dcur" "$dnew" || true)"
        [[ "$okd" == "true" ]] && log "[cleanup] drone $assignee liberato" || yellow "[cleanup] drone $assignee: CAS mancata (race?)"
      fi
    fi

    if (( done == 0 )); then
      red "[cleanup] delivery $id: CAS fallita (race con dispatcher/drone?)"
      fail=$((fail+1))
    fi
  done <<< "$ids"

  if (( fail == 0 )); then
    green "[cleanup] tutte le $total consegne aperte chiuse ✔"
  else
    red "[cleanup] completate con errori ($fail fallite su $total)"
    return 1
  fi
}

