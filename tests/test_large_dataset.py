"""Prueba de integración con millones de eventos sintéticos.

Valida el requisito crítico de memoria (>= 70% menos RAM que la carga cruda) y
compara las métricas contra la verdad conocida del generador.

Tamaño por defecto: 2 millones de eventos. Para la corrida completa:
    READING_TEST_EVENTS=5000000 pytest tests/test_large_dataset.py
"""
import os

import numpy as np
import pandas as pd
import pytest

from benchmark_memory import compare
from generate_data import make_catalog, make_events
from reading_pipeline.run import run

N_EVENTS = int(os.environ.get("READING_TEST_EVENTS", 2_000_000))


@pytest.fixture(scope="module")
def dataset(tmp_path_factory):
    d = tmp_path_factory.mktemp("large")
    rng = np.random.default_rng(7)
    make_catalog(20_000, rng).to_csv(d / "catalog.csv", index=False)
    ev, truth = make_events(N_EVENTS, 200_000, 20_000, 0.04, rng)
    ev.to_csv(d / "logs.csv", index=False, date_format="%Y-%m-%d %H:%M:%S")
    last = ev.groupby(["user_id", "book_id"])["event_ts"].max().rename("last_ts").reset_index()
    truth = truth.merge(last, on=["user_id", "book_id"], how="left")
    truth.attrs["max_ts"] = ev["event_ts"].max()
    return d, len(ev), truth


def test_memory_reduction_at_least_70pct(dataset):
    d, n, _ = dataset
    rep = compare(d / "logs.csv", d / "catalog.csv")
    assert rep["raw"]["rows"] == rep["optimized"]["rows"] == n
    # Mismo paso (cargar el CSV): DataFrame y pico de RAM del proceso.
    assert rep["dataframe_reduction_pct"] >= 70, rep
    assert rep["peak_rss_reduction_pct"] >= 70, rep
    # Pico del pipeline completo (cargar, deduplicar, sesionar, agregar, escribir)
    # contra solo la carga cruda.
    assert rep["pipeline_peak_vs_raw_load_reduction_pct"] >= 70, rep


def test_pipeline_end_to_end_matches_ground_truth(dataset):
    d, n, truth = dataset
    report = run(d / "logs.csv", d / "catalog.csv", d / "out")
    assert report["rows_in"] == n
    # El generador inyecta ~4% de duplicados.
    assert 0.03 < report["duplicates_removed"] / n < 0.05

    truth = truth[~truth.duplicated(["user_id", "book_id"], keep=False)]
    key = ["user_id", "book_id"]
    drop = pd.read_parquet(d / "out" / "dropoff_by_user_book.parquet")
    binge = pd.read_parquet(d / "out" / "binge_by_user_book.parquet")

    drop_profiles = drop[key].merge(truth, on=key)["profile"]
    assert (drop_profiles == "dropoff_mid").mean() > 0.99  # precisión
    cutoff = truth.attrs["max_ts"] - pd.Timedelta(days=30)
    expected = truth[(truth["profile"] == "dropoff_mid") & (truth["last_ts"] <= cutoff)]
    found = expected[key].merge(drop[key], on=key)
    assert len(found) / len(expected) > 0.99  # exhaustividad (recall)
    binge_profiles = binge[key].merge(truth, on=key)["profile"]
    true_binges = (truth["profile"] == "binge").sum()
    assert (binge_profiles == "binge").sum() / true_binges > 0.99  # exhaustividad
    assert binge_profiles.isin(["binge", "finisher"]).all()

    for f in ["reading_time_by_user_book", "dropoff_by_book", "binge_by_book"]:
        assert len(pd.read_parquet(d / "out" / f"{f}.parquet")) > 0
