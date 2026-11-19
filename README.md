# Pipeline de consumo de lectura y retención

Pipeline en Pandas que limpia los logs de lectura, los cruza con el catálogo de
libros y genera métricas de tiempo de lectura, abandono (drop-off) y lectura
continua (binge-reading), con carga optimizada en memoria.

## Uso rápido

```bash
pip install -r requirements.txt
python scripts/generate_data.py --events 5000000 --out data/raw      # datos sintéticos
python scripts/benchmark_memory.py --logs data/raw/reading_logs.csv --out data/output
python -m reading_pipeline.run --logs data/raw/reading_logs.csv \
    --catalog data/raw/catalog.csv --out data/output
pytest                                            # 2M eventos por defecto
READING_TEST_EVENTS=5000000 pytest               # corrida completa
```

## Esquemas de entrada (supuestos, ajustar a los datos reales)

| Logs (`reading_logs.csv`) | Tipo optimizado |
|---|---|
| `event_id` | no se carga (la dedup es por clave natural) |
| `user_id`, `book_id` | `uint32` / downcast al menor entero sin signo |
| `event_type` (open, progress, close, finish) | `category` |
| `event_ts`, `ingested_at` (`YYYY-MM-DD HH:MM:SS`) | `datetime64[s]` |
| `progress_pct` (0 a 100) | `float32` al cargar, `uint8` tras validar |
| `device`, `app_version` | `category` |

Catálogo (`catalog.csv`): `book_id`, `title`, `author`, `genre`, `language`,
`total_pages`, `published_year`.

## Reglas de negocio (parámetros en `reading_pipeline/config.py`)

- **CA 1 Deduplicación:** clave natural `(user_id, book_id, event_type, event_ts, progress_pct)`;
  se conserva la primera ingesta. Cubre reintentos con el mismo `event_id` o con uno nuevo.
- **CA 2 Tiempo de lectura:** sesiones por usuario y libro; una pausa de más de 30 min abre
  sesión nueva. El tiempo es la suma de intervalos dentro de las sesiones (las pausas no cuentan,
  y el tiempo posterior al último evento de una sesión no es observable).
- **CA 3 Abandono:** no terminado, progreso máximo en [40%, 60%] y sin actividad en los 30 días
  previos a la fecha de referencia (por defecto el último evento del dataset; `--reference-date`).
- **CA 4 Binge:** terminado (evento `finish` o progreso ≥ 95%) en 1 sesión o en ≤ 24 h desde la
  primera apertura.

## Salidas (Parquet, en `--out`)

| Archivo | Contenido |
|---|---|
| `reading_time_by_user_book` | una fila por usuario y libro: minutos/horas de lectura, sesiones, progreso, flags |
| `dropoff_by_user_book` | abandonos detectados, con datos del catálogo |
| `dropoff_by_book` | lectores, abandonos, terminados, tasa de abandono y % medio de abandono por libro |
| `binge_by_user_book` | lecturas de un tirón: horas de lectura, lapso y sesiones |
| `binge_by_book` | terminaciones, terminaciones binge y tasa binge por libro |
| `run_report.json` | filas inválidas, duplicados eliminados, eventos huérfanos, conteos y config usada |

## Resultado de memoria (4.78 M eventos, 446 MB de CSV)

| | Carga cruda | Carga optimizada | Reducción |
|---|---|---|---|
| DataFrame (`memory_usage(deep=True)`) | 654 MB | 132 MB | **79.8%** |
| Pico de RAM del proceso (VmHWM) | 1,823 MB | 479 MB | **73.7%** |

La línea base es `pd.read_csv(path)` sin indicar tipos. El pipeline completo sobre ese
dataset tarda unos 12 s.

## Pruebas

- `tests/test_pipeline_cases.py`: casos de negocio construidos a mano (reintentos, sesiones
  partidas, abandono en rango/fuera de rango/en curso, binge por sesión única y por lapso corto).
- `tests/test_large_dataset.py`: millones de eventos sintéticos; valida la reducción ≥ 70% de RAM
  y compara abandono y binge contra la verdad conocida del generador.
