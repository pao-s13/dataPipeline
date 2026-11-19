"""Transformaciones y métricas de negocio (CA 1 a CA 4).

Las operaciones pesadas se hacen sobre arreglos NumPy y se reemplazan columnas de
a una, para que el pico de RAM del pipeline completo se mantenga cerca del tamaño
del DataFrame optimizado.
"""
import numpy as np
import pandas as pd

from .config import PipelineConfig

# Columnas que necesitan las métricas; device y app_version se descartan antes de ordenar.
METRIC_COLUMNS = ["user_id", "book_id", "event_type", "event_ts", "progress_pct"]


def _reorder(df: pd.DataFrame, idx: np.ndarray) -> pd.DataFrame:
    """Equivale a df.iloc[idx], pero copia una columna a la vez y libera la original."""
    out = {}
    for c in list(df.columns):
        col = df.pop(c)
        out[c] = col.array.take(idx) if isinstance(col.dtype, pd.CategoricalDtype) \
            else col.to_numpy()[idx]
        del col
    return pd.DataFrame(out, copy=False)


def deduplicate(df: pd.DataFrame, cfg: PipelineConfig) -> tuple[pd.DataFrame, dict]:
    """CA 1: elimina reintentos de sincronización.

    Un reintento puede llegar con el mismo event_id o con uno nuevo, por eso se
    compara por clave natural. Se ordena por (usuario, libro, tiempo, tipo,
    progreso, ingesta): los duplicados quedan contiguos y el primero es la primera
    ingesta. Un progreso nulo se compara como igual a otro nulo. El resultado queda
    ordenado como lo necesita la sesionización.

    Consume el DataFrame de entrada (le quita columnas) para no tener dos copias
    completas en memoria mientras se reordena.
    """
    n0 = len(df)
    for c in [c for c in df.columns if c not in METRIC_COLUMNS and c != "ingested_at"]:
        df.pop(c)  # libera device/app_version: las métricas no las usan
    sort_cols = ["user_id", "book_id", "event_ts"] + [
        c for c in cfg.dedup_key if c not in ("user_id", "book_id", "event_ts")]
    keys = {c: df[c].array.codes if c == "event_type" else df[c].to_numpy() for c in sort_cols}
    if "progress_pct" in keys:
        keys["progress_pct"] = np.nan_to_num(keys["progress_pct"], nan=-1.0)
    # np.lexsort usa la última clave como la principal.
    order = np.lexsort([df.pop("ingested_at").to_numpy()] + [keys[c] for c in reversed(sort_cols)])
    dup = np.zeros(n0, dtype=bool)
    dup[1:] = True
    for c in cfg.dedup_key:
        k = keys.pop(c)[order]
        dup[1:] &= k[1:] == k[:-1]
        del k
    keys.clear()
    df = _reorder(df, order[~dup])
    return df, {"duplicates_removed": int(dup.sum()), "rows_after_dedup": len(df)}


def fill_progress(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Completa progresos nulos (p. ej. un `open` sin progreso) y los pasa a uint8.

    Dentro de cada par usuario-libro se toma el último progreso conocido; si es el
    primer evento, el siguiente. Pares sin ningún progreso conocido se descartan.
    Requiere df ordenado por (user_id, book_id, event_ts).
    """
    new_pair = _new_pair(df)
    pair = np.cumsum(new_pair) - 1
    p = pd.Series(df["progress_pct"].to_numpy())
    if p.isna().any():
        g = p.groupby(pair, sort=False)
        p = g.ffill().fillna(p.groupby(pair, sort=False).bfill())
    keep = p.notna().to_numpy()
    df["progress_pct"] = p.fillna(0).round().to_numpy().astype("uint8")
    dropped = int((~keep).sum())
    if dropped:
        df = df.loc[keep].reset_index(drop=True)
    return df, {"rows_without_any_progress": dropped}


def _new_pair(df: pd.DataFrame) -> np.ndarray:
    u, b = df["user_id"].to_numpy(), df["book_id"].to_numpy()
    out = np.ones(len(df), dtype=bool)
    out[1:] = (u[1:] != u[:-1]) | (b[1:] != b[:-1])
    return out


def sessionize(df: pd.DataFrame, cfg: PipelineConfig) -> pd.DataFrame:
    """CA 2: marca inicios de sesión y el tiempo de lectura de cada intervalo.

    Requiere df ordenado por (user_id, book_id, event_ts). Una pausa mayor a
    `session_gap_minutes` abre una sesión nueva; esa pausa no cuenta como lectura.
    Tampoco cuenta el tiempo entre un `close` y el evento siguiente (aunque sea
    dentro de los 30 min): el usuario cerró el libro.
    """
    ts = df["event_ts"].to_numpy().astype("int64")
    gap = np.zeros(len(df), dtype="int64")
    gap[1:] = ts[1:] - ts[:-1]
    new_pair = _new_pair(df)
    new_session = new_pair | (gap > cfg.session_gap_minutes * 60)
    closed_before = np.zeros(len(df), dtype=bool)
    closed_before[1:] = df["event_type"].array.codes[:-1] == df["event_type"].dtype.categories.get_loc("close")
    df["pair_id"] = (np.cumsum(new_pair) - 1).astype("uint32")
    df["session_start"] = new_session
    df["read_seconds"] = np.where(new_session | closed_before, 0, gap).astype("float32")
    return df


def user_book_metrics(df: pd.DataFrame, cfg: PipelineConfig) -> pd.DataFrame:
    """Una fila por (usuario, libro) con tiempo de lectura, sesiones y progreso."""
    pair = df["pair_id"].to_numpy()
    n_pairs = int(pair[-1]) + 1 if len(pair) else 0
    starts = np.flatnonzero(_new_pair(df))
    ends = np.append(starts[1:], len(df)) - 1
    ts = df["event_ts"].to_numpy()
    prog = df["progress_pct"].to_numpy()
    et = df["event_type"].array
    is_finish = et.codes == et.categories.get_loc("finish")
    reached = is_finish | (prog >= cfg.finished_pct)

    m = pd.DataFrame({
        "user_id": df["user_id"].to_numpy()[starts],
        "book_id": df["book_id"].to_numpy()[starts],
        "first_ts": ts[starts],
        "last_ts": ts[ends],
        "n_events": (ends - starts + 1).astype("uint32"),
        "n_sessions": np.bincount(pair, weights=df["session_start"].to_numpy(),
                                  minlength=n_pairs).astype("uint32"),
        "start_progress_pct": prog[starts],
        "max_progress_pct": np.maximum.reduceat(prog, starts) if n_pairs else prog[:0],
    })
    read_s = np.bincount(pair, weights=df["read_seconds"].to_numpy(), minlength=n_pairs)
    m["reading_minutes"] = (read_s / 60).round(2).astype("float32")
    m["reading_hours"] = (read_s / 3600).round(3).astype("float32")
    m["span_hours"] = ((m["last_ts"] - m["first_ts"]).dt.total_seconds() / 3600).astype("float32")

    # Primer momento en que se terminó el libro (evento finish o progreso >= umbral).
    ts_i = ts.astype("int64")
    fin_i = np.full(n_pairs, np.iinfo("int64").max)
    idx = np.flatnonzero(reached)  # df está ordenado por tiempo dentro de cada par
    first = idx[np.r_[True, pair[idx][1:] != pair[idx][:-1]]] if len(idx) else idx
    fin_i[pair[first]] = ts_i[first]
    finished = fin_i != np.iinfo("int64").max
    m["finished"] = finished
    m["finished_ts"] = pd.to_datetime(np.where(finished, fin_i, np.iinfo("int64").min)
                                      .astype("datetime64[s]"))
    before_fin = df["session_start"].to_numpy() & (ts_i <= fin_i[pair])
    m["sessions_to_finish"] = np.bincount(pair, weights=before_fin, minlength=n_pairs).astype("uint32")
    m["hours_to_finish"] = ((m["finished_ts"] - m["first_ts"]).dt.total_seconds() / 3600).astype("float32")
    return m


def tag_dropoff(m: pd.DataFrame, cfg: PipelineConfig, reference_ts=None) -> pd.DataFrame:
    """CA 3: marca abandonos en el rango definido por negocio (límites incluidos).

    Abandono = no terminado, progreso máximo dentro de [min, max] y sin actividad
    durante `dropoff_inactivity_days` antes de la fecha de referencia (por defecto,
    el último evento del dataset). Así una lectura en curso no se cuenta como abandono.
    """
    ref = pd.Timestamp(reference_ts) if reference_ts is not None else m["last_ts"].max()
    inactive = m["last_ts"] <= ref - pd.Timedelta(days=cfg.dropoff_inactivity_days)
    in_range = m["max_progress_pct"].between(cfg.dropoff_min_pct, cfg.dropoff_max_pct)
    m["is_dropoff"] = ~m["finished"] & in_range & inactive
    return m


def tag_binge(m: pd.DataFrame, cfg: PipelineConfig) -> pd.DataFrame:
    """CA 4: libros terminados de un tirón.

    Se exige que la lectura observada empiece cerca del inicio del libro
    (`binge_max_start_pct`) y que se haya leído tiempo real, para no contar un
    `finish` aislado o una sesión que arranca en 85%. Las sesiones y el lapso se
    miden hasta el momento en que se terminó: reabrir el libro días después no
    anula el binge.
    """
    quick = ((m["sessions_to_finish"] <= cfg.binge_max_sessions)
             | (m["hours_to_finish"] <= cfg.binge_max_span_hours))
    covered = (m["start_progress_pct"] <= cfg.binge_max_start_pct) & (m["reading_minutes"] > 0)
    m["is_binge"] = m["finished"] & covered & quick
    return m


def _with_catalog(df: pd.DataFrame, catalog: pd.DataFrame) -> pd.DataFrame:
    cols = ["book_id", "title", "author", "genre", "language", "total_pages"]
    return df.merge(catalog[cols], on="book_id", how="left", validate="many_to_one")


def dropoff_outputs(m: pd.DataFrame, catalog: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    detail_cols = ["user_id", "book_id", "max_progress_pct", "reading_minutes",
                   "n_sessions", "first_ts", "last_ts"]
    detail = _with_catalog(m.loc[m["is_dropoff"], detail_cols], catalog)

    by_book = m.groupby("book_id", observed=True).agg(
        readers=("user_id", "size"),
        dropoffs=("is_dropoff", "sum"),
        finished=("finished", "sum"),
    ).reset_index()
    by_book["dropoff_rate"] = (by_book["dropoffs"] / by_book["readers"]).astype("float32")
    avg = detail.groupby("book_id")["max_progress_pct"].mean().rename("avg_dropoff_pct")
    by_book = by_book.merge(avg, on="book_id", how="left")
    by_book = _with_catalog(by_book, catalog)
    by_book = by_book.sort_values(["dropoffs", "dropoff_rate"], ascending=False, ignore_index=True)
    return detail.reset_index(drop=True), by_book


def binge_outputs(m: pd.DataFrame, catalog: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    detail_cols = ["user_id", "book_id", "reading_hours", "hours_to_finish",
                   "sessions_to_finish", "first_ts", "finished_ts"]
    detail = _with_catalog(m.loc[m["is_binge"], detail_cols], catalog).reset_index(drop=True)

    fin = m.loc[m["finished"]]
    by_book = fin.groupby("book_id", observed=True).agg(
        completions=("user_id", "size"),
        binge_completions=("is_binge", "sum"),
        median_reading_hours=("reading_hours", "median"),
    ).reset_index()
    by_book["binge_rate"] = (by_book["binge_completions"] / by_book["completions"]).astype("float32")
    by_book = by_book.loc[by_book["binge_completions"] > 0]
    by_book = _with_catalog(by_book, catalog)
    by_book = by_book.sort_values(["binge_completions", "binge_rate"], ascending=False, ignore_index=True)
    return detail, by_book
