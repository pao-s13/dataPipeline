#!/usr/bin/env bash
# Descarga el subconjunto de poesía de Goodreads (UCSD Book Graph, ~230 MB) y lo
# convierte al formato de entrada del pipeline.
#
# Uso: bash scripts/get_goodreads.sh
# Resultado: data/goodreads_pipeline/reading_logs.csv y data/goodreads_pipeline/catalog.csv
set -euo pipefail

BASE=https://mcauleylab.ucsd.edu/public_datasets/gdrive/goodreads/byGenre
RAW=data/goodreads
OUT=data/goodreads_pipeline

mkdir -p "$RAW"
for f in books interactions reviews; do
  file="goodreads_${f}_poetry.json.gz"
  if [ -s "$RAW/$file" ]; then
    echo "Ya existe $RAW/$file"
  else
    echo "Descargando $file"
    curl -fL --retry 3 -o "$RAW/$file" "$BASE/$file"
  fi
done

python scripts/goodreads_adapter.py --genre poetry --raw "$RAW" --out "$OUT"
