#!/usr/bin/env bash
set -e

# Spegni tutto e rimuovi volumi
docker compose down -v

# Ricostruisci immagini e avvia i servizi
docker compose up -d --build --scale gateway=3


echo "⏳ Attendo che i servizi si stabilizzino..."
sleep 30
