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

- **Validación:** filas con IDs nulos, no numéricos o negativos, `event_type` desconocido, fecha
  ilegible o progreso fuera de [0, 100] se descartan y se cuentan en `run_report.json`. Un progreso
  nulo (p. ej. un `open` sin progreso) se conserva y se completa con el último progreso conocido del
  par usuario-libro (o el siguiente, si es el primer evento). En el catálogo se descartan IDs nulos y
  se deja un registro por `book_id`.
- **CA 1 Deduplicación:** clave natural `(user_id, book_id, event_type, event_ts, progress_pct)`;
  se conserva la primera ingesta. Cubre reintentos con el mismo `event_id` o con uno nuevo.
- **CA 2 Tiempo de lectura:** sesiones por usuario y libro; una pausa de **más de** 30 min abre
  sesión nueva (30 min exactos siguen en la misma). El tiempo es la suma de intervalos dentro de las
  sesiones; no cuentan las pausas largas ni el tiempo entre un `close` y el evento siguiente.
- **CA 3 Abandono:** no terminado, progreso máximo en [40%, 60%] (límites incluidos) y sin actividad
  en los 30 días previos a la fecha de referencia (por defecto el último evento; `--reference-date`).
- **CA 4 Binge:** terminado (evento `finish` o progreso ≥ 95%), con la lectura observada empezando en
  ≤ 10% y tiempo de lectura real, y además terminado en 1 sesión o en ≤ 24 h desde la primera
  apertura. Sesiones y lapso se miden **hasta el momento en que se terminó**, así que reabrir el libro
  después no anula el binge.

## Salidas (Parquet, en `--out`)

| Archivo | Contenido |
|---|---|
| `reading_time_by_user_book` | una fila por usuario y libro: minutos/horas de lectura, sesiones, progreso, flags |
| `dropoff_by_user_book` | abandonos detectados, con datos del catálogo |
| `dropoff_by_book` | lectores, abandonos, terminados, tasa de abandono y % medio de abandono por libro |
| `binge_by_user_book` | lecturas de un tirón: horas de lectura, lapso y sesiones |
| `binge_by_book` | terminaciones, terminaciones binge y tasa binge por libro |
| `run_report.json` | filas inválidas, duplicados eliminados, eventos huérfanos, conteos y config usada |

## Resultado de memoria (4.78 M eventos, 446 MB de CSV, versiones de `requirements.txt`)

| | Carga cruda | Optimizado | Reducción |
|---|---|---|---|
| DataFrame de logs (`memory_usage(deep=True)`) | 654 MB | 132 MB | **79.8%** |
| Pico de RAM al cargar (VmHWM del proceso) | 1,181 MB | 195 MB | **83.5%** |
| Pico de RAM del **pipeline completo** vs. solo la carga cruda | 1,181 MB | 275 MB | **76.7%** |

La línea base es `pd.read_csv(path)` sin indicar tipos. El pico del pipeline completo incluye
cargar, deduplicar, sesionar, agregar y escribir Parquet. Reproducir con:

```bash
python scripts/benchmark_memory.py --logs data/raw/reading_logs.csv --catalog data/raw/catalog.csv
```

Cómo se logra: lectura por bloques con validación y conversión a tipos compactos en cada bloque,
arreglos finales reservados de antemano (sin concatenar), y en el pipeline, reordenamiento de a una
columna con `np.lexsort` en lugar de `sort_values` + `drop_duplicates` sobre el DataFrame entero.

Las versiones están fijadas porque el pico de la carga cruda cambia entre versiones de pandas (con
pandas 3.0.5 la carga cruda llega a 1,819 MB; con 3.1.1, a 1,181 MB). La meta del 70% se verificó con
ambas. El pipeline completo tarda unos 15 s.

## Pruebas

- `tests/test_pipeline_cases.py`: casos de negocio construidos a mano (reintentos, sesiones
  partidas, abandono en rango/fuera de rango/en curso, binge por sesión única y por lapso corto).
- `tests/test_edge_cases.py`: datos sucios (IDs nulos o no numéricos, evento desconocido, fecha
  ilegible, catálogo con IDs duplicados o nulos, `open` sin progreso) y límites exactos: 30 min,
  40% y 60%, 30 días de inactividad, 24 h, close y reapertura, `finish` aislado, arranque en 85%,
  reapertura después de terminar y binge con pausas.
- `tests/test_large_dataset.py`: millones de eventos sintéticos; valida la reducción ≥ 70% en la
  carga y en el pipeline completo, y mide precisión y exhaustividad de abandono y binge contra la
  verdad conocida del generador.

## Datos reales: Goodreads

`scripts/goodreads_adapter.py` convierte los datasets públicos de Goodreads (UCSD Book Graph) al
esquema del pipeline, y `--preset sparse` ajusta las reglas para fuentes con solo fecha de inicio y
de fin. Mapeo, resultados de la corrida con poesía y limitaciones en [docs/goodreads.md](docs/goodreads.md).
