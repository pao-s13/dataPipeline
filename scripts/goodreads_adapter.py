"""Adaptador: datasets públicos de Goodreads (UCSD Book Graph) -> esquema del pipeline.

Fuente: https://mcauleylab.ucsd.edu/public_datasets/gdrive/goodreads/ (carpeta byGenre/).
Archivos por género, por ejemplo poesía:
    goodreads_books_poetry.json.gz         catálogo
    goodreads_interactions_poetry.json.gz  una fila por usuario-libro (shelves, fechas)
    goodreads_reviews_poetry.json.gz       reseñas (subconjunto de las interacciones)

Mapeo a eventos (Goodreads no tiene telemetría de lectura, solo fechas por libro):
    started_at                 -> event_type=open,   progress_pct=0
    read_at (si is_read)       -> event_type=finish, progress_pct=100
    date_added / date_updated  -> no son eventos de lectura (agregar a un estante,
                                  editar la reseña); date_updated se usa como ingested_at
    device                     -> archivo de origen ("interactions" o "reviews")

Interacciones y reseñas se exportan por separado y repiten las mismas fechas de
lectura: al combinarlas aparecen duplicados reales, que es justo lo que resuelve CA 1.

Uso:
    python scripts/goodreads_adapter.py --genre poetry --raw data/goodreads --out data/goodreads_pipeline
    python -m reading_pipeline.run --preset sparse \
        --logs data/goodreads_pipeline/reading_logs.csv \
        --catalog data/goodreads_pipeline/catalog.csv --out data/goodreads_output
"""
import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

GR_TS_FORMAT = "%a %b %d %H:%M:%S %Y"
# Goodreads se lanzó en 2007; fechas anteriores son lecturas cargadas a mano después
# (no comportamiento observado). El scraping del dataset terminó a fines de 2017.
VALID_FROM = pd.Timestamp("2007-01-01")
VALID_TO = pd.Timestamp("2017-12-31 23:59:59")
OUT_TS_FORMAT = "%Y-%m-%d %H:%M:%S"
EVENT_COLS = ["user_id", "book_id", "is_read", "date_updated", "read_at", "started_at"]


def parse_gr_ts(s: pd.Series) -> pd.Series:
    """Fechas de Goodreads ('Tue Mar 01 00:00:00 -0800 1983') en hora local del usuario.

    Se descarta el offset en vez de convertir a UTC: así una fecha cargada sin hora
    (00:00:00 local) sigue reconociéndose como "solo fecha", y las horas del día
    corresponden a la hora local de lectura.
    """
    s = s.fillna("").str.replace(r" [+-]\d{4} ", " ", regex=True)
    return pd.to_datetime(s.replace("", None), format=GR_TS_FORMAT, errors="coerce")


def read_jsonl(path: Path, columns: list[str], chunksize: int = 500_000):
    for ch in pd.read_json(path, lines=True, dtype=False, chunksize=chunksize):
        yield ch.reindex(columns=columns)


def events_from(path: Path, source: str, stats: dict) -> pd.DataFrame:
    parts = []
    for ch in read_jsonl(path, EVENT_COLS):
        stats[f"{source}_rows"] = stats.get(f"{source}_rows", 0) + len(ch)
        # Las reseñas no traen is_read: una reseña con read_at es un libro leído.
        is_read = (pd.Series(True, index=ch.index) if source == "reviews"
                   else ch["is_read"].astype(bool))
        started, read = parse_gr_ts(ch["started_at"]), parse_gr_ts(ch["read_at"])
        updated = parse_gr_ts(ch["date_updated"])
        read = read.where(is_read)
        for etype, ts, pct in (("open", started, 0), ("finish", read, 100)):
            ok = ts.notna()
            parts.append(pd.DataFrame({
                "user_id": ch.loc[ok, "user_id"], "book_id": ch.loc[ok, "book_id"],
                "event_type": etype, "event_ts": ts[ok], "progress_pct": pct,
                "device": source, "app_version": "goodreads_ucsd",
                "ingested_at": updated[ok].fillna(ts[ok]),
            }))
    return pd.concat(parts, ignore_index=True)


def build_catalog(path: Path) -> pd.DataFrame:
    cols = ["book_id", "title", "authors", "language_code", "num_pages", "publication_year",
            "popular_shelves"]
    rows = []
    for ch in read_jsonl(path, cols):
        rows.append(pd.DataFrame({
            "book_id": ch["book_id"],
            "title": ch["title"],
            "author": ch["authors"].map(lambda a: a[0]["author_id"] if isinstance(a, list) and a else ""),
            # Género: el estante popular más usado que no sea de estado de lectura.
            "genre": ch["popular_shelves"].map(_top_shelf),
            "language": ch["language_code"].replace("", "unknown"),
            "total_pages": pd.to_numeric(ch["num_pages"], errors="coerce"),
            "published_year": pd.to_numeric(ch["publication_year"], errors="coerce"),
        }))
    return pd.concat(rows, ignore_index=True)


_STATUS_SHELVES = {"to-read", "currently-reading", "read", "favorites", "owned", "books-i-own",
                   "default", "wish-list", "library", "to-buy", "kindle", "ebook", "own"}


def _top_shelf(shelves) -> str:
    if not isinstance(shelves, list):
        return "unknown"
    for s in shelves:
        name = s.get("name", "")
        if name and name not in _STATUS_SHELVES:
            return name
    return "unknown"


def adapt(raw: Path, genre: str, out: Path) -> dict:
    """Genera reading_logs.csv, catalog.csv, user_id_map.csv y adapter_report.json en `out`."""
    out.mkdir(parents=True, exist_ok=True)
    stats: dict = {"genre": genre}

    ev = pd.concat([
        events_from(raw / f"goodreads_interactions_{genre}.json.gz", "interactions", stats),
        events_from(raw / f"goodreads_reviews_{genre}.json.gz", "reviews", stats),
    ], ignore_index=True)

    in_window = ev["event_ts"].between(VALID_FROM, VALID_TO)
    stats["events_outside_2007_2017_dropped"] = int((~in_window).sum())
    ev = ev.loc[in_window].reset_index(drop=True)

    # IDs de usuario de Goodreads son hashes hexadecimales: se mapean a enteros.
    codes, uniques = pd.factorize(ev["user_id"])
    ev["user_id"] = (codes + 1).astype("uint32")
    pd.DataFrame({"user_id": np.arange(1, len(uniques) + 1), "goodreads_user_id": uniques}) \
        .to_csv(out / "user_id_map.csv", index=False)

    stats.update({
        "events": len(ev),
        "events_open": int((ev["event_type"] == "open").sum()),
        "events_finish": int((ev["event_type"] == "finish").sum()),
        "users": int(ev["user_id"].nunique()),
        "books_with_events": int(ev["book_id"].nunique()),
        "min_event_ts": str(ev["event_ts"].min()),
        "max_event_ts": str(ev["event_ts"].max()),
        "events_date_only_pct": round(100 * (ev["event_ts"] == ev["event_ts"].dt.normalize()).mean(), 1),
    })
    ev = ev.sort_values("ingested_at", kind="stable")
    ev.insert(0, "event_id", np.arange(len(ev)))
    ev.to_csv(out / "reading_logs.csv", index=False, date_format=OUT_TS_FORMAT)

    cat = build_catalog(raw / f"goodreads_books_{genre}.json.gz")
    stats["catalog_books"] = len(cat)
    cat.to_csv(out / "catalog.csv", index=False)
    (out / "adapter_report.json").write_text(json.dumps(stats, indent=2))
    return stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--genre", default="poetry")
    ap.add_argument("--raw", type=Path, default=Path("data/goodreads"))
    ap.add_argument("--out", type=Path, default=Path("data/goodreads_pipeline"))
    a = ap.parse_args()
    print(json.dumps(adapt(a.raw, a.genre, a.out), indent=2))


if __name__ == "__main__":
    main()
