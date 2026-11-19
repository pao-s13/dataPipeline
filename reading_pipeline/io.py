"""Carga de datos: versión cruda (línea base) y versión optimizada en memoria.

Técnicas de optimización aplicadas en `load_logs`:
  * `usecols`: solo se leen las columnas que usa el pipeline (event_id no se
    necesita porque la deduplicación es por clave natural).
  * Lectura por bloques (`chunksize`): cada bloque se valida y se convierte a
    tipos compactos antes de leer el siguiente, así el CSV nunca está entero en
    memoria como texto.
  * Tipos compactos: IDs a enteros sin signo, `event_type`/`device`/`app_version`
    a `category`, progreso a `float32` (luego `uint8`), fechas a `datetime64[s]`.
  * Arreglos finales reservados de antemano (se cuentan las líneas del CSV): cada
    bloque se escribe en su lugar, sin concatenar, para no tener dos copias del
    DataFrame en memoria.

La validación también ocurre por bloque: filas con IDs nulos o no numéricos,
`event_type` desconocido, fecha ilegible o progreso fuera de [0, 100] se descartan
y se cuentan. Un progreso nulo se conserva (p. ej. un `open` sin progreso).
"""
from pathlib import Path

import numpy as np
import pandas as pd

EVENT_TYPES = pd.CategoricalDtype(["open", "progress", "close", "finish"])
TS_FORMAT = "%Y-%m-%d %H:%M:%S"
UINT32_MAX = np.iinfo("uint32").max

LOG_COLUMNS = ["user_id", "book_id", "event_type", "event_ts", "progress_pct",
               "device", "app_version", "ingested_at"]
READ_DTYPES = {
    "user_id": "str", "book_id": "str", "event_type": "category",
    "event_ts": "str", "progress_pct": "str", "device": "category",
    "app_version": "category", "ingested_at": "str",
}


def load_raw(path: Path) -> pd.DataFrame:
    """Carga cruda, sin ninguna indicación de tipos (línea base de memoria)."""
    return pd.read_csv(path)


def _to_id(s: pd.Series) -> pd.Series:
    v = pd.to_numeric(s, errors="coerce")
    return v.where((v >= 0) & (v <= UINT32_MAX) & (v == np.floor(v)))


def _clean_chunk(ch: pd.DataFrame, stats: dict) -> dict:
    """Valida un bloque y lo devuelve como arreglos compactos por columna."""
    uid, bid = _to_id(ch["user_id"]), _to_id(ch["book_id"])
    # Tipos de evento desconocidos quedan como nulos y se descartan.
    ch["event_type"] = ch["event_type"].cat.set_categories(EVENT_TYPES.categories)
    ts = pd.to_datetime(ch["event_ts"], format=TS_FORMAT, errors="coerce")
    ing = pd.to_datetime(ch["ingested_at"], format=TS_FORMAT, errors="coerce")
    prog = pd.to_numeric(ch["progress_pct"], errors="coerce").astype("float32")

    bad_key = uid.isna() | bid.isna() | ch["event_type"].isna()
    bad_ts = ts.isna() & ~bad_key
    bad_prog = prog.notna() & ~prog.between(0, 100) & ~bad_key & ~bad_ts
    ok = ~(bad_key | bad_ts | bad_prog)
    stats["rows_in"] += len(ch)
    stats["invalid_key"] += int(bad_key.sum())
    stats["invalid_timestamp"] += int(bad_ts.sum())
    stats["invalid_progress"] += int(bad_prog.sum())
    stats["missing_progress"] += int((prog.isna() & ok).sum())

    ok = ok.to_numpy()
    ts = ts.to_numpy()[ok].astype("datetime64[s]")
    ing = ing.to_numpy()[ok].astype("datetime64[s]")
    # Si falta la fecha de ingesta, se usa la del evento (orden de llegada desconocido).
    ing = np.where(np.isnat(ing), ts, ing)
    return {
        "user_id": uid.to_numpy()[ok].astype("uint32"),
        "book_id": bid.to_numpy()[ok].astype("uint32"),
        "event_type": ch["event_type"].array[ok],
        "event_ts": ts,
        "progress_pct": prog.to_numpy()[ok],
        "device": ch["device"].array[ok],
        "app_version": ch["app_version"].array[ok],
        "ingested_at": ing,
    }


def _release_freed_memory_eagerly() -> None:
    """En Linux/glibc, devuelve al sistema los bloques liberados de más de 128 KB.

    Sin esto, glibc sube su umbral de mmap tras liberar arreglos grandes y la RAM
    de los bloques ya procesados queda retenida por el proceso.
    """
    try:
        import ctypes
        ctypes.CDLL("libc.so.6").mallopt(-3, 128 * 1024)  # M_MMAP_THRESHOLD
    except (OSError, AttributeError):
        pass


def _downcast_uint(a: np.ndarray) -> np.ndarray:
    """Downcasting al menor entero sin signo que admite el máximo de la columna."""
    top = int(a.max()) if len(a) else 0
    for t in ("uint8", "uint16", "uint32"):
        if top <= np.iinfo(t).max:
            return a.astype(t, copy=False)
    return a


def _count_lines(path: Path, block: int = 1 << 24) -> int:
    n = 0
    with open(path, "rb") as f:
        while b := f.read(block):
            n += b.count(b"\n")
    return n + 1


class _CategoryBuilder:
    """Acumula códigos de una columna categórica con categorías que crecen por bloque."""

    def __init__(self, capacity: int):
        self.codes = np.empty(capacity, dtype="int16")
        self.categories: list = []
        self._pos: dict = {}

    def put(self, start: int, values: pd.Categorical) -> None:
        for c in values.categories:
            if c not in self._pos:
                self._pos[c] = len(self.categories)
                self.categories.append(c)
        if len(self.categories) > np.iinfo("int16").max:
            raise ValueError("demasiadas categorías para int16")
        remap = np.array([self._pos[c] for c in values.categories] + [-1], dtype="int16")
        # código -1 (nulo) se mapea al último elemento de remap, que es -1.
        self.codes[start:start + len(values)] = remap[values.codes]

    def build(self, n: int) -> pd.Categorical:
        return pd.Categorical.from_codes(self.codes[:n], categories=self.categories)


def load_logs(path: Path, chunksize: int = 100_000) -> tuple[pd.DataFrame, dict]:
    """Carga optimizada y validada de los logs. Devuelve (df, estadísticas de calidad).

    Se cuenta primero el número de líneas para reservar los arreglos finales y
    cada bloque se escribe directamente en ellos: no hay lista de bloques ni
    concatenación, así el pico de RAM es el tamaño final más un bloque.
    """
    stats = dict(rows_in=0, invalid_key=0, invalid_timestamp=0,
                 invalid_progress=0, missing_progress=0)
    _release_freed_memory_eagerly()
    cap = _count_lines(path)
    out = {
        "user_id": np.empty(cap, "uint32"), "book_id": np.empty(cap, "uint32"),
        "event_type": np.empty(cap, "int8"), "event_ts": np.empty(cap, "datetime64[s]"),
        "progress_pct": np.empty(cap, "float32"), "ingested_at": np.empty(cap, "datetime64[s]"),
    }
    cats = {"device": _CategoryBuilder(cap), "app_version": _CategoryBuilder(cap)}
    n = 0
    reader = pd.read_csv(path, usecols=LOG_COLUMNS, dtype=READ_DTYPES, chunksize=chunksize)
    for ch in reader:
        part = _clean_chunk(ch, stats)
        k = len(part["user_id"])
        for c, arr in out.items():
            src = part[c].codes if c == "event_type" else part[c]
            arr[n:n + k] = src
        for c, b in cats.items():
            b.put(n, part[c])
        n += k
        del ch, part

    cols = {}
    for c in LOG_COLUMNS:
        if c in cats:
            cols[c] = cats[c].build(n)
        elif c == "event_type":
            cols[c] = pd.Categorical.from_codes(out[c][:n], dtype=EVENT_TYPES)
        elif c in ("user_id", "book_id"):
            cols[c] = _downcast_uint(out[c][:n])
        else:
            cols[c] = out[c][:n]
        out.pop(c, None)
    df = pd.DataFrame(cols, copy=False)
    stats["invalid_rows"] = stats["invalid_key"] + stats["invalid_timestamp"] + stats["invalid_progress"]
    return df, stats


def load_catalog(path: Path) -> tuple[pd.DataFrame, dict]:
    """Carga el catálogo, descarta IDs nulos y deja un solo registro por book_id."""
    df = pd.read_csv(path, dtype={"genre": "category", "language": "category"})
    n0 = len(df)
    df["book_id"] = _to_id(df["book_id"])
    df = df.dropna(subset=["book_id"])
    n_null = n0 - len(df)
    dup = df["book_id"].duplicated(keep="first")
    df = df.loc[~dup].reset_index(drop=True)
    df["book_id"] = pd.to_numeric(df["book_id"].astype("uint32"), downcast="unsigned")
    for c in ("total_pages", "published_year"):
        df[c] = pd.to_numeric(df[c], errors="coerce").round().astype("UInt16")
    return df, {"catalog_rows_in": n0, "catalog_invalid_id": int(n_null),
                "catalog_duplicate_id": int(dup.sum())}


def memory_mb(df: pd.DataFrame) -> float:
    return df.memory_usage(deep=True).sum() / 1024**2


def memory_by_column(df: pd.DataFrame) -> pd.Series:
    return (df.memory_usage(deep=True, index=False) / 1024**2).round(2)
