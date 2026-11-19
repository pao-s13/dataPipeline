# Corrida con datos reales: Goodreads (UCSD Book Graph), género poesía

## Dataset

Fuente: <https://mcauleylab.ucsd.edu/public_datasets/gdrive/goodreads/byGenre/> (los enlaces de
Google Drive de la página del dataset ya no funcionan; los archivos están en esa carpeta).

| Archivo | Filas |
|---|---|
| `goodreads_interactions_poetry.json.gz` (152 MB) | 2,734,350 interacciones usuario-libro |
| `goodreads_reviews_poetry.json.gz` (49 MB) | 154,555 reseñas |
| `goodreads_books_poetry.json.gz` (28 MB) | 36,514 libros |

Después del adaptador: **908,692 eventos** de 122,012 usuarios sobre 33,972 libros (2007-2017).

## Mapeo al esquema del pipeline

Goodreads no es telemetría de una app: por cada usuario-libro solo trae fechas.

| Goodreads | Pipeline |
|---|---|
| `started_at` | `event_type=open`, `progress_pct=0` |
| `read_at` (si `is_read`) | `event_type=finish`, `progress_pct=100` |
| `date_updated` | `ingested_at` |
| archivo de origen (`interactions` / `reviews`) | `device` |
| `user_id` (hash hex) | entero (`user_id_map.csv`) |
| `date_added` (agregar a un estante) | no se usa: no es un evento de lectura |
| libros: `book_id`, `title`, primer `author_id`, `language_code`, `num_pages`, `publication_year` | catálogo |
| primer estante popular que no sea de estado (`to-read`, `currently-reading`, …) | `genre` |

Se descartan 30,355 eventos con fechas fuera de 2007-2017 (Goodreads nació en 2007 y el scraping
terminó en 2017; antes son lecturas cargadas a mano después, y hay fechas imposibles como el año 2558).
Las fechas se dejan en hora local del usuario: el 50% no tiene hora (00:00:00), solo fecha.

## Reglas ajustadas (`--preset sparse`, `PipelineConfig.sparse_events()`)

| CA | Logs de app | Goodreads | Por qué |
|---|---|---|---|
| CA 1 | clave natural | igual | Las reseñas repiten las fechas de las interacciones: son duplicados reales. |
| CA 2 | suma de intervalos ≤ 30 min | igual, pero **no es medible** | Con solo inicio y fin separados por horas o días, el tiempo de lectura sale 0 en el 95.6% de los pares. Se reporta el lapso inicio → fin (`hours_to_finish`). |
| CA 3 | progreso máx. en 40-60% e inactivo 30 días | rango **[0, 0]** e inactivo 30 días | No hay % intermedio. El proxy es "empezó, nunca terminó y lleva 30+ días sin actividad". |
| CA 4 | empieza en ≤ 10%, tiempo de lectura > 0 y (1 sesión o ≤ 24 h) | igual pero **sin exigir tiempo de lectura** | El tiempo de lectura no es medible; el binge se decide por el lapso ≤ 24 h. |

## Resultados

| Métrica | Valor |
|---|---|
| Eventos de entrada | 908,692 |
| Duplicados eliminados (reseña = interacción) | 191,600 (21%) |
| Pares usuario-libro | 448,710 |
| Terminados | 400,259 (de ellos 131,923 solo con fecha de fin) |
| Binge (≤ 24 h de inicio a fin) | 106,176 pares, 17,333 libros |
| … de esos, inicio y fin en el mismo día sin hora (lapso 0) | 24,578 |
| … con hora real en ambos eventos | 40,722 (mediana 0.6 h) |
| Abandono proxy (empezó y no terminó, 30+ días inactivo) | 48,451 pares |
| Lapso inicio → fin (mediana de terminados con inicio) | 52 h |

Libros con más binge: *Milk and Honey* (6,257 de 11,761 terminaciones, 53%), *The Princess Saves
Herself in this One* (67%) y *Love & Misadventure* (45%): poesía contemporánea y corta.
Libros con más abandono (≥ 200 lectores): *The Divine Comedy* (66% en una de sus ediciones, 40% en
la más leída), *The Prophet* (49%) y *The Book of Disquiet* (43%): clásicos largos o densos.

## Memoria (908,692 filas, CSV de 88 MB, pandas 3.1.1)

| | Carga cruda | Optimizado | Reducción |
|---|---|---|---|
| DataFrame | 121.4 MB | 26.9 MB | 77.9% |
| Pico de RAM al cargar | 241.2 MB | 72.0 MB | 70.1% |
| Pico del pipeline completo vs. solo la carga cruda | 241.2 MB | 160.2 MB | 33.6% |

Con menos de un millón de filas pesan los costos fijos (catálogo con títulos, escritura de Parquet,
pyarrow), por eso el pipeline completo queda en 33.6%. Con 4.78 M eventos sintéticos llega a 76.7%.

## Limitaciones

- No hay progreso intermedio: el abandono a mitad del libro (40-60%) no se puede medir con esta fuente.
- El tiempo de lectura (CA 2) no es medible; `hours_to_finish` es tiempo calendario, no lectura.
- La mitad de las fechas no tiene hora: "mismo día" cuenta como lapso 0 y "al día siguiente" como 24 h.
- `binge_rate` por libro usa todas las terminaciones, incluidas las que solo tienen fecha de fin, que
  nunca pueden ser binge; la tasa real entre lectores con fecha de inicio es mayor.
- El abandono proxy no distingue entre abandonar y terminar sin registrarlo en Goodreads.

## Cómo correrlo

```bash
mkdir -p data/goodreads && cd data/goodreads
B=https://mcauleylab.ucsd.edu/public_datasets/gdrive/goodreads/byGenre
for f in books interactions reviews; do curl -O $B/goodreads_${f}_poetry.json.gz; done
cd ../..
python scripts/goodreads_adapter.py --genre poetry --raw data/goodreads --out data/goodreads_pipeline
python -m reading_pipeline.run --preset sparse --logs data/goodreads_pipeline/reading_logs.csv \
    --catalog data/goodreads_pipeline/catalog.csv --out data/goodreads_output
python scripts/benchmark_memory.py --logs data/goodreads_pipeline/reading_logs.csv \
    --catalog data/goodreads_pipeline/catalog.csv
```
