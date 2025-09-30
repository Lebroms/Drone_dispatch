#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$ROOT/lib.sh"

# usage helper
usage() {
  cat <<EOF
Usage: $0 [--reuse]
  (no args)  fresh run: down -v + up (gateway x3), poi applica overrides e piano
  --reuse     NON tocca i container: solo overrides + delivery plan
EOF
}

REUSE=0
if [[ "${1:-}" == "--reuse" ]]; then
  REUSE=1
elif [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage; exit 0
fi

compose_up(){
  section "Pulizia ambiente precedente"
  docker compose down -v --remove-orphans || true

  section "Avvio stack (senza ordergen_bursty) con gateway x3"
  docker compose stop ordergen_bursty >/dev/null 2>&1 || true
  docker compose rm -f ordergen_bursty >/dev/null 2>&1 || true

  # un solo up con scaling (gateway x3), poi lb
  docker compose up -d \
    rabbitmq kvstore_a kvstore_b kvstore_c kvfront \
    dispatcher drone \
    --scale gateway=3
  docker compose up -d lb

  section "Attendo readiness"
  wait_health "http://localhost:9000/health" "kvfront"
  wait_health "http://localhost:8080/health" "LB (ingresso test)"
  curl -sf "http://localhost:8080/zones" >/dev/null
  green "Servizi pronti ✔"
}

attach_only(){
  section "Verifico che i servizi siano up (modalità --reuse)"
  wait_health "http://localhost:9000/health" "kvfront"
  wait_health "http://localhost:8080/health" "LB (ingresso test)"
  curl -sf "http://localhost:8080/zones" >/dev/null
  green "Servizi raggiungibili ✔"
}

main(){
  if (( REUSE == 0 )); then
    compose_up
  else
    attach_only
  fi

  section "Dashboard"
  echo "Apri: http://localhost:8080/dashboard"

  section "Applico DRONE_OVERRIDES"
  apply_drone_overrides
  nap 5

  section "Eseguo DELIVERY_PLAN"
  apply_delivery_plan


  echo "=== Logs decisioni charging (live) ==="
  docker compose logs -f dispatcher | grep --line-buffered '\[charge\]'

  green "Esecuzione completata ✔"
}

main "$@"
