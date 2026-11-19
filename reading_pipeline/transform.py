"""Transformaciones y métricas de negocio (CA 1 a CA 4)."""
import numpy as np
import pandas as pd

from .config import PipelineConfig


def clean(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Descarta eventos inválidos (claves nulas, fechas ilegibles, progreso fuera de rango)."""
    n0 = len(df)
    valid = (
        df[["user_id", "book_id", "event_type", "event_ts"]].notna().all(axis=1)
        & df["progress_pct"].between(0, 100)
    )
    df = df.loc[valid].copy()
    df["progress_pct"] = df["progress_pct"].round().astype("uint8")
    return df, {"rows_in": n0, "invalid_rows": int(n0 - len(df))}


def deduplicate(df: pd.DataFrame, cfg: PipelineConfig) -> tuple[pd.DataFrame, dict]:
    """CA 1: elimina reintentos de sincronización.

    Un reintento puede llegar con el mismo event_id o con uno nuevo, por eso se
    compara por clave natural. Se conserva la primera ingesta. El resultado queda
    ordenado por usuario, libro y tiempo, que es lo que necesita la sesionización.
    """
    n0 = len(df)
    df = df.sort_values(["user_id", "book_id", "event_ts", "ingested_at"], kind="stable")
    df = df.drop_duplicates(subset=list(cfg.dedup_key), keep="first").reset_index(drop=True)
    return df, {"duplicates_removed": int(n0 - len(df)), "rows_after_dedup": len(df)}


def sessionize(df: pd.DataFrame, cfg: PipelineConfig) -> pd.DataFrame:
    """CA 2: asigna `session_id` y el tiempo de lectura de cada intervalo.

    Requiere df ordenado por (user_id, book_id, event_ts). Una pausa mayor a
    `session_gap_minutes` abre una sesión nueva; esa pausa no cuenta como lectura.
    """
    same_pair = (df["user_id"].eq(df["user_id"].shift())
                 & df["book_id"].eq(df["book_id"].shift()))
    gap_s = df["event_ts"].diff().dt.total_seconds().astype("float32")
    new_session = ~same_pair | (gap_s > cfg.session_gap_minutes * 60)
    df["session_start"] = new_session
    df["session_id"] = new_session.cumsum().astype("uint32")
    df["read_seconds"] = gap_s.where(~new_session, 0).astype("float32")
    return df


def user_book_metrics(df: pd.DataFrame, cfg: PipelineConfig) -> pd.DataFrame:
    """Una fila por (usuario, libro) con tiempo de lectura, sesiones y progreso."""
    df = df.assign(is_finish=df["event_type"].eq("finish"))
    g = df.groupby(["user_id", "book_id"], sort=False, observed=True)
    m = g.agg(
        first_ts=("event_ts", "min"),
        last_ts=("event_ts", "max"),
        n_events=("event_ts", "size"),
        n_sessions=("session_start", "sum"),
        read_seconds=("read_seconds", "sum"),
        max_progress_pct=("progress_pct", "max"),
        has_finish_event=("is_finish", "any"),
    ).reset_index()
    m["reading_minutes"] = (m["read_seconds"] / 60).round(2).astype("float32")
    m["reading_hours"] = (m["read_seconds"] / 3600).round(3).astype("float32")
    m["span_hours"] = ((m["last_ts"] - m["first_ts"]).dt.total_seconds() / 3600).astype("float32")
    m["finished"] = m["has_finish_event"] | (m["max_progress_pct"] >= cfg.finished_pct)
    m["n_sessions"] = m["n_sessions"].astype("uint32")
    m["n_events"] = m["n_events"].astype("uint32")
    return m.drop(columns=["read_seconds", "has_finish_event"])


def tag_dropoff(m: pd.DataFrame, cfg: PipelineConfig, reference_ts=None) -> pd.DataFrame:
    """CA 3: marca abandonos en el rango definido por negocio.

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
    """CA 4: libros terminados de un tirón (pocas sesiones o lapso corto)."""
    quick = (m["n_sessions"] <= cfg.binge_max_sessions) | (m["span_hours"] <= cfg.binge_max_span_hours)
    m["is_binge"] = m["finished"] & quick
    return m


def _with_catalog(df: pd.DataFrame, catalog: pd.DataFrame) -> pd.DataFrame:
    cols = ["book_id", "title", "author", "genre", "language", "total_pages"]
    return df.merge(catalog[cols], on="book_id", how="left")


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
    detail_cols = ["user_id", "book_id", "reading_hours", "span_hours", "n_sessions",
                   "first_ts", "last_ts"]
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
