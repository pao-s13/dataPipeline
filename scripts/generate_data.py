"""Generador de datos sintéticos: catálogo de libros + logs de lectura.

Simula "recorridos" (un usuario leyendo un libro) con perfiles conocidos:
lectores normales que terminan, binge-readers, abandonos en 40-60%, abandonos
tempranos y lecturas en curso. Luego inyecta duplicados por reintentos de
sincronización (mismo event_id o event_id nuevo) y desordena la llegada.

Uso:
    python scripts/generate_data.py --events 5000000 --out data/raw
"""
import argparse
import time
from pathlib import Path

import numpy as np
import pandas as pd

GENRES = ["fantasia", "ciencia_ficcion", "romance", "thriller", "historia",
          "ensayo", "infantil", "poesia", "biografia", "autoayuda"]
LANGS = ["es", "en", "pt", "fr"]
DEVICES = ["ios", "android", "web", "kindle", "tablet"]
APP_VERSIONS = ["3.1.0", "3.2.0", "3.2.1", "3.3.0", "4.0.0"]

PERIOD_START = np.datetime64("2026-01-01T00:00:00", "s")
PERIOD_DAYS = 180

# perfil: (probabilidad, (min_eventos, max_eventos), prob. de corte de sesión)
PROFILES = {
    "finisher": (0.30, (30, 90), 0.08),
    "binge": (0.07, (60, 160), 0.0),
    "dropoff_mid": (0.18, (15, 50), 0.10),
    "dropoff_early": (0.25, (3, 20), 0.15),
    "in_progress": (0.20, (10, 50), 0.10),
}


def make_catalog(n_books: int, rng: np.random.Generator) -> pd.DataFrame:
    ids = np.arange(1, n_books + 1)
    return pd.DataFrame({
        "book_id": ids,
        "title": [f"Libro {i}" for i in ids],
        "author": [f"Autor {i % (n_books // 4 + 1)}" for i in ids],
        "genre": rng.choice(GENRES, n_books),
        "language": rng.choice(LANGS, n_books, p=[0.6, 0.25, 0.1, 0.05]),
        "total_pages": rng.integers(60, 900, n_books),
        "published_year": rng.integers(1950, 2027, n_books),
    })


def make_events(n_events: int, n_users: int, n_books: int, dup_rate: float,
                rng: np.random.Generator) -> pd.DataFrame:
    names = list(PROFILES)
    probs = np.array([PROFILES[p][0] for p in names])
    mean_events = sum(p * (lo + hi) / 2 for p, (lo, hi), _ in PROFILES.values())
    n_journeys = int(n_events / (1 + dup_rate) / mean_events)

    profile = rng.choice(len(names), n_journeys, p=probs)
    lo = np.array([PROFILES[p][1][0] for p in names])[profile]
    hi = np.array([PROFILES[p][1][1] for p in names])[profile]
    n_ev = rng.integers(lo, hi + 1)
    p_break = np.array([PROFILES[p][2] for p in names])[profile]

    # Progreso final de cada recorrido según su perfil.
    p_end = np.select(
        [profile == names.index("finisher"), profile == names.index("binge"),
         profile == names.index("dropoff_mid"), profile == names.index("dropoff_early")],
        [100, 100, rng.integers(40, 61, n_journeys), rng.integers(1, 35, n_journeys)],
        default=rng.integers(5, 90, n_journeys),
    )

    # Momento de inicio: los abandonos ocurren lejos del final del período;
    # las lecturas en curso, en los últimos días (no deben marcarse como abandono).
    start_day = rng.uniform(0, PERIOD_DAYS - 45, n_journeys)
    in_prog = profile == names.index("in_progress")
    start_day[in_prog] = rng.uniform(PERIOD_DAYS - 10, PERIOD_DAYS - 3, in_prog.sum())
    start = PERIOD_START + (start_day * 86400).astype("int64").astype("timedelta64[s]")

    user = rng.integers(1, n_users + 1, n_journeys)
    # Popularidad sesgada de libros (Zipf truncado).
    book = (rng.zipf(1.3, n_journeys) - 1) % n_books + 1

    # Expansión a nivel evento.
    j = np.repeat(np.arange(n_journeys), n_ev)
    total = j.size
    first = np.zeros(total, dtype=bool)
    offsets = np.concatenate([[0], np.cumsum(n_ev)[:-1]])
    first[offsets] = True
    k = np.arange(total) - np.repeat(offsets, n_ev)  # índice dentro del recorrido

    # Intervalos: lectura continua (10s-20min) o corte de sesión (2h-5 días).
    gap = np.clip(rng.exponential(100, total) + 10, 10, 20 * 60)
    brk = (rng.random(total) < p_break[j]) & ~first
    gap[brk] = rng.uniform(2 * 3600, 5 * 86400, brk.sum())
    gap[first] = 0
    gap = gap.astype("int64")
    cum = np.cumsum(gap)
    cum -= np.repeat(cum[offsets], n_ev)
    ts = start[j] + cum.astype("timedelta64[s]")

    denom = np.maximum(n_ev - 1, 1)[j]
    progress = np.round(p_end[j] * k / denom).astype("int64")

    last = np.zeros(total, dtype=bool)
    last[offsets + n_ev - 1] = True
    next_brk = np.zeros(total, dtype=bool)
    next_brk[:-1] = brk[1:]
    etype = np.full(total, "progress", dtype=object)
    etype[next_brk | last] = "close"
    etype[first | brk] = "open"
    etype[last & (p_end[j] == 100)] = "finish"

    ev = pd.DataFrame({
        "user_id": user[j],
        "book_id": book[j],
        "event_type": etype,
        "event_ts": ts,
        "progress_pct": progress,
        "device": np.array(DEVICES)[rng.integers(0, len(DEVICES), n_journeys)][j],
        "app_version": np.array(APP_VERSIONS)[rng.integers(0, len(APP_VERSIONS), n_journeys)][j],
    })
    # El extracto de logs tiene fecha de corte: no existen eventos posteriores.
    period_end = PERIOD_START + np.timedelta64(PERIOD_DAYS, "D")
    ev = ev.loc[ev["event_ts"] < period_end].reset_index(drop=True)
    total = len(ev)
    ev["ingested_at"] = ev["event_ts"] + pd.to_timedelta(rng.integers(1, 120, total), unit="s")
    ev["event_id"] = rng.integers(0, 2**63, total, dtype=np.int64)

    # Duplicados por reintentos: mitad con mismo event_id, mitad con uno nuevo.
    n_dup = int(total * dup_rate)
    idx = rng.choice(total, n_dup, replace=False)
    dups = ev.iloc[idx].copy()
    new_id = rng.random(n_dup) < 0.5
    dups.loc[new_id, "event_id"] = rng.integers(0, 2**63, new_id.sum(), dtype=np.int64)
    dups["ingested_at"] = dups["ingested_at"] + pd.to_timedelta(rng.integers(60, 6 * 3600, n_dup), unit="s")
    ev = pd.concat([ev, dups], ignore_index=True)

    # Llegada desordenada (por fecha de ingesta, con algo de ruido).
    ev = ev.sort_values("ingested_at", kind="stable").reset_index(drop=True)
    truth = pd.DataFrame({"user_id": user, "book_id": book,
                          "profile": np.array(names)[profile], "final_pct": p_end})
    ev["event_id"] = ev["event_id"].map("{:016x}".format)
    cols = ["event_id", "user_id", "book_id", "event_type", "event_ts", "progress_pct",
            "device", "app_version", "ingested_at"]
    return ev[cols], truth


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=5_000_000)
    ap.add_argument("--users", type=int, default=200_000)
    ap.add_argument("--books", type=int, default=20_000)
    ap.add_argument("--dup-rate", type=float, default=0.04)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", type=Path, default=Path("data/raw"))
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed)
    args.out.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    make_catalog(args.books, rng).to_csv(args.out / "catalog.csv", index=False)
    ev, truth = make_events(args.events, args.users, args.books, args.dup_rate, rng)
    truth.to_csv(args.out / "ground_truth_journeys.csv", index=False)
    ev.to_csv(args.out / "reading_logs.csv", index=False, date_format="%Y-%m-%d %H:%M:%S")
    print(f"{len(ev):,} eventos y {args.books:,} libros en {args.out} ({time.time() - t0:.1f}s)")


if __name__ == "__main__":
    main()
