"""Carga de datos: versión cruda (línea base) y versión optimizada en memoria.

Técnicas de optimización aplicadas en `load_logs`:
  * `usecols`: solo se leen las columnas que usa el pipeline (event_id no se
    necesita porque la deduplicación es por clave natural).
  * `dtype` explícito al parsear: IDs a enteros sin signo de 32 bits, textos de
    baja cardinalidad a `category`, progreso a `float32` (luego `uint8`).
  * Fechas a `datetime64[s]` en lugar de texto.
  * Lectura por bloques (`chunksize`) para que el pico de RAM no dependa de
    tener el CSV entero como texto en memoria.
  * Downcasting final de enteros al tipo más chico que admite su rango.
"""
from pathlib import Path

import pandas as pd
from pandas.api.types import union_categoricals

EVENT_TYPES = pd.CategoricalDtype(["open", "progress", "close", "finish"])
TS_FORMAT = "%Y-%m-%d %H:%M:%S"

LOG_COLUMNS = ["user_id", "book_id", "event_type", "event_ts", "progress_pct",
               "device", "app_version", "ingested_at"]
LOG_DTYPES = {
    "user_id": "uint32",
    "book_id": "uint32",
    "event_type": EVENT_TYPES,
    "progress_pct": "float32",
    "device": "category",
    "app_version": "category",
}
CATALOG_DTYPES = {
    "book_id": "uint32",
    "genre": "category",
    "language": "category",
    "total_pages": "uint16",
    "published_year": "uint16",
}


def load_raw(path: Path) -> pd.DataFrame:
    """Carga cruda, sin ninguna indicación de tipos (línea base de memoria)."""
    return pd.read_csv(path)


def _concat_chunks(chunks: list[pd.DataFrame]) -> pd.DataFrame:
    """Concatena bloques preservando columnas categóricas con categorías distintas."""
    cat_cols = [c for c, t in chunks[0].dtypes.items()
                if isinstance(t, pd.CategoricalDtype)]
    unified = {c: union_categoricals([ch[c] for ch in chunks]) for c in cat_cols}
    out = pd.concat([ch.drop(columns=cat_cols) for ch in chunks], ignore_index=True)
    for c in cat_cols:
        out[c] = unified[c]
    return out


def load_logs(path: Path, chunksize: int = 250_000) -> pd.DataFrame:
    """Carga optimizada de los logs de lectura."""
    chunks = []
    reader = pd.read_csv(
        path, usecols=LOG_COLUMNS, dtype=LOG_DTYPES,
        parse_dates=["event_ts", "ingested_at"], date_format=TS_FORMAT,
        chunksize=chunksize,
    )
    for ch in reader:
        for c in ("event_ts", "ingested_at"):
            ch[c] = pd.to_datetime(ch[c], errors="coerce").astype("datetime64[s]")
        chunks.append(ch)
    df = _concat_chunks(chunks)[LOG_COLUMNS]
    df["book_id"] = pd.to_numeric(df["book_id"], downcast="unsigned")
    df["user_id"] = pd.to_numeric(df["user_id"], downcast="unsigned")
    return df


def load_catalog(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=CATALOG_DTYPES)
    df["book_id"] = pd.to_numeric(df["book_id"], downcast="unsigned")
    return df


def memory_mb(df: pd.DataFrame) -> float:
    return df.memory_usage(deep=True).sum() / 1024**2


def memory_by_column(df: pd.DataFrame) -> pd.Series:
    return (df.memory_usage(deep=True, index=False) / 1024**2).round(2)
