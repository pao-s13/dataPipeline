"""Orquestación del pipeline completo.

Uso:
    python -m reading_pipeline.run --logs data/raw/reading_logs.csv \
        --catalog data/raw/catalog.csv --out data/output
"""
import argparse
import json
import time
from dataclasses import asdict
from pathlib import Path

from . import io, transform
from .config import PipelineConfig


def run(logs_path: Path, catalog_path: Path, out_dir: Path,
        cfg: PipelineConfig = PipelineConfig(), reference_ts=None) -> dict:
    t0 = time.time()
    out_dir.mkdir(parents=True, exist_ok=True)

    logs, q = io.load_logs(logs_path)
    catalog, qc = io.load_catalog(catalog_path)
    report = {"config": asdict(cfg), "logs_memory_mb": round(io.memory_mb(logs), 2), **q, **qc}
    report["orphan_events"] = int((~logs["book_id"].isin(catalog["book_id"])).sum())

    logs, d = transform.deduplicate(logs, cfg)
    report.update(d)
    logs, f = transform.fill_progress(logs)
    report.update(f)

    logs = transform.sessionize(logs, cfg)
    metrics = transform.user_book_metrics(logs, cfg)
    del logs
    metrics = transform.tag_dropoff(metrics, cfg, reference_ts)
    metrics = transform.tag_binge(metrics, cfg)

    drop_detail, drop_book = transform.dropoff_outputs(metrics, catalog)
    binge_detail, binge_book = transform.binge_outputs(metrics, catalog)

    outputs = {
        "reading_time_by_user_book": metrics,
        "dropoff_by_user_book": drop_detail,
        "dropoff_by_book": drop_book,
        "binge_by_user_book": binge_detail,
        "binge_by_book": binge_book,
    }
    for name, df in outputs.items():
        df.to_parquet(out_dir / f"{name}.parquet", index=False)
        report[f"rows_{name}"] = len(df)

    report["user_book_pairs"] = len(metrics)
    report["seconds"] = round(time.time() - t0, 1)
    (out_dir / "run_report.json").write_text(json.dumps(report, indent=2, default=str))
    return report


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--logs", type=Path, required=True)
    ap.add_argument("--catalog", type=Path, required=True)
    ap.add_argument("--out", type=Path, default=Path("data/output"))
    ap.add_argument("--session-gap-minutes", type=int, default=30)
    ap.add_argument("--dropoff-min", type=float, default=40)
    ap.add_argument("--dropoff-max", type=float, default=60)
    ap.add_argument("--dropoff-inactivity-days", type=int, default=30)
    ap.add_argument("--binge-max-span-hours", type=float, default=24)
    ap.add_argument("--binge-max-start-pct", type=float, default=10)
    ap.add_argument("--reference-date", default=None,
                    help="Fecha de corte para el abandono (por defecto, el último evento)")
    a = ap.parse_args()
    cfg = PipelineConfig(
        session_gap_minutes=a.session_gap_minutes,
        dropoff_min_pct=a.dropoff_min, dropoff_max_pct=a.dropoff_max,
        dropoff_inactivity_days=a.dropoff_inactivity_days,
        binge_max_span_hours=a.binge_max_span_hours,
        binge_max_start_pct=a.binge_max_start_pct,
    )
    print(json.dumps(run(a.logs, a.catalog, a.out, cfg, a.reference_date), indent=2, default=str))


if __name__ == "__main__":
    main()
