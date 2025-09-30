#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
force_finish_all_open_deliveries_strict
