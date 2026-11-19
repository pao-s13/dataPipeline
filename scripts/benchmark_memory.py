"""Compara el consumo de RAM de la carga cruda vs. la carga optimizada.

Cada modo corre en un subproceso aparte para medir también el pico de RAM del
proceso (VmHWM), no solo el tamaño del DataFrame resultante.

Uso:
    python scripts/benchmark_memory.py --logs data/raw/reading_logs.csv --out data/output
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

CHILD = r"""
import json, sys
sys.path.insert(0, {root!r})
import pandas as pd
from reading_pipeline import io

def status_kb(field):
    # VmHWM es el pico de RAM residente del proceso; a diferencia de ru_maxrss,
    # no hereda el pico del proceso padre.
    for line in open("/proc/self/status"):
        if line.startswith(field + ":"):
            return int(line.split()[1])

base = status_kb("VmRSS")
if {mode!r} == "pipeline":
    import tempfile
    from pathlib import Path
    from reading_pipeline.run import run
    with tempfile.TemporaryDirectory() as d:
        run(Path({path!r}), Path({catalog!r}), Path(d))
    peak = status_kb("VmHWM")
    print(json.dumps({{"peak_rss_mb": round(peak / 1024, 1),
                      "peak_rss_delta_mb": round((peak - base) / 1024, 1)}}))
    sys.exit(0)
df = io.load_raw({path!r}) if {mode!r} == "raw" else io.load_logs({path!r})[0]
peak = status_kb("VmHWM")
print(json.dumps({{
    "rows": len(df),
    "dataframe_mb": round(io.memory_mb(df), 2),
    "peak_rss_mb": round(peak / 1024, 1),
    "peak_rss_delta_mb": round((peak - base) / 1024, 1),
    "by_column_mb": io.memory_by_column(df).to_dict(),
    "dtypes": {{c: str(t) for c, t in df.dtypes.items()}},
}}))
"""


def measure(path: Path, mode: str, catalog: Path | None = None) -> dict:
    code = CHILD.format(root=str(ROOT), path=str(path), mode=mode, catalog=str(catalog))
    res = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    return json.loads(res.stdout)


def compare(path: Path, catalog: Path | None = None) -> dict:
    """Mide la carga cruda, la carga optimizada y (si hay catálogo) el pipeline completo.

    La reducción de carga compara el mismo paso (leer el CSV a un DataFrame). El
    pipeline completo se reporta aparte, contra la misma línea base de carga cruda,
    porque además de cargar ordena, deduplica y agrega.
    """
    raw, opt = measure(path, "raw"), measure(path, "optimized")
    extra = {}
    if catalog is not None:
        pipe = measure(path, "pipeline", catalog)
        extra = {"pipeline": pipe, "pipeline_peak_vs_raw_load_reduction_pct": round(
            100 * (1 - pipe["peak_rss_delta_mb"] / raw["peak_rss_delta_mb"]), 1)}
    return {
        **extra,
        "raw": raw,
        "optimized": opt,
        "dataframe_reduction_pct": round(100 * (1 - opt["dataframe_mb"] / raw["dataframe_mb"]), 1),
        "peak_rss_reduction_pct": round(100 * (1 - opt["peak_rss_delta_mb"] / raw["peak_rss_delta_mb"]), 1),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--logs", type=Path, required=True)
    ap.add_argument("--catalog", type=Path, default=None,
                    help="Si se indica, mide también el pico del pipeline completo")
    ap.add_argument("--out", type=Path, default=None)
    a = ap.parse_args()
    rep = compare(a.logs, a.catalog)
    print(f"Filas: {rep['raw']['rows']:,}")
    print(f"DataFrame crudo:      {rep['raw']['dataframe_mb']:>9.1f} MB")
    print(f"DataFrame optimizado: {rep['optimized']['dataframe_mb']:>9.1f} MB")
    print(f"Reducción DataFrame:  {rep['dataframe_reduction_pct']:>9.1f} %")
    print(f"Pico RAM crudo:       {rep['raw']['peak_rss_delta_mb']:>9.1f} MB")
    print(f"Pico RAM optimizado:  {rep['optimized']['peak_rss_delta_mb']:>9.1f} MB")
    print(f"Reducción pico RAM:   {rep['peak_rss_reduction_pct']:>9.1f} %")
    if "pipeline" in rep:
        print(f"Pico RAM pipeline completo: {rep['pipeline']['peak_rss_delta_mb']:>5.1f} MB "
              f"({rep['pipeline_peak_vs_raw_load_reduction_pct']:.1f} % menos que solo la carga cruda)")
    if a.out:
        a.out.mkdir(parents=True, exist_ok=True)
        (a.out / "memory_report.json").write_text(json.dumps(rep, indent=2))


if __name__ == "__main__":
    main()
