#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$ROOT/lib.sh"

section "Emergency: reset flotta 1..15 (inactive + battery 100)"
apply_backup_overrides
