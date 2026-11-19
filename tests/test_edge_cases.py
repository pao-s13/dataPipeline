"""Casos adversarios y de límites: datos sucios, umbrales exactos y reglas de CA 2 y CA 4."""
import pandas as pd
import pytest

from reading_pipeline.run import run

REF = pd.Timestamp("2026-06-30 00:00:00")
T0 = pd.Timestamp("2026-03-01 10:00:00")
OLD = REF - pd.Timedelta(days=60)
S = pd.Timedelta(seconds=1)


def m(x):
    return pd.Timedelta(minutes=x)


def h(x):
    return pd.Timedelta(hours=x)


def ev(uid, bid, etype, ts, pct):
    return {"event_id": "x", "user_id": uid, "book_id": bid, "event_type": etype,
            "event_ts": ts, "progress_pct": pct, "device": "web", "app_version": "4.0.0",
            "ingested_at": ts + pd.Timedelta(seconds=5)}


def read_session(uid, bid, start, p0, p1, minutes, step=10):
    """Lectura continua de p0 a p1 con un evento cada `step` minutos."""
    n = max(minutes // step, 1)
    rows = [ev(uid, bid, "open", start, p0)]
    for k in range(1, n + 1):
        pct = round(p0 + (p1 - p0) * k / n)
        etype = "finish" if (k == n and p1 == 100) else ("close" if k == n else "progress")
        rows.append(ev(uid, bid, etype, start + m(step * k), pct))
    return rows


def build_events():
    r = []
    # --- CA 2: límites de sesión y close ---
    r += [ev(101, 1, "open", T0, 0), ev(101, 1, "progress", T0 + m(30), 5)]       # 30 min exactos
    r += [ev(102, 1, "open", T0, 0), ev(102, 1, "progress", T0 + m(30) + S, 5)]   # 30 min + 1 s
    r += [ev(103, 1, "open", T0, 0), ev(103, 1, "close", T0 + m(10), 10),         # close y reapertura
          ev(103, 1, "open", T0 + m(20), 10), ev(103, 1, "close", T0 + m(30), 20)]

    # --- CA 3: límites de rango y de inactividad ---
    for uid, pct in [(111, 40), (112, 60), (113, 39), (114, 61)]:
        r += [ev(uid, 2, "open", OLD, 0), ev(uid, 2, "close", OLD + m(20), pct)]
    edge = REF - pd.Timedelta(days=30)
    r += [ev(115, 2, "open", edge - m(20), 0), ev(115, 2, "close", edge, 50)]          # 30 días exactos
    r += [ev(116, 2, "open", edge - m(20), 0), ev(116, 2, "close", edge + S, 50)]      # 1 s menos

    # --- CA 4: límites del lapso y casos adversarios ---
    r += read_session(121, 3, T0, 0, 50, 60) + read_session(121, 3, T0 + h(23), 50, 100, 60)  # 24 h exactas
    r += read_session(122, 3, T0, 0, 50, 60) + read_session(122, 3, T0 + h(23) + S, 50, 100, 60)
    # Binge con pausas: close y reapertura a los 10 min, y una pausa de 25 min.
    r += read_session(201, 4, T0, 0, 40, 60)
    r += read_session(201, 4, T0 + m(70), 40, 70, 60)
    r += read_session(201, 4, T0 + m(155), 70, 100, 60)
    r += [ev(203, 4, "finish", T0, 100)]                        # finish aislado
    r += read_session(204, 4, T0, 85, 100, 20)                  # empieza en 85%
    r += read_session(205, 4, T0, 0, 100, 180)                  # binge...
    r += [ev(205, 4, "open", T0 + pd.Timedelta(days=5), 100)]   # ...y reabre 5 días después
    r += [ev(206, 4, "open", T0, None), ev(206, 4, "progress", T0 + m(10), 10)]  # open sin progreso
    return pd.DataFrame(r)


def dirty_rows():
    """Filas inválidas que no deben romper la carga ni llegar a las métricas."""
    base = dict(event_id="x", event_type="progress", event_ts="2026-03-01 10:00:00",
                progress_pct="10", device="web", app_version="4.0.0",
                ingested_at="2026-03-01 10:00:05")
    return pd.DataFrame([
        {**base, "user_id": "", "book_id": "1"},
        {**base, "user_id": "7", "book_id": ""},
        {**base, "user_id": "abc", "book_id": "1"},
        {**base, "user_id": "-5", "book_id": "1"},
        {**base, "user_id": "7", "book_id": "1", "event_type": "foo"},
        {**base, "user_id": "7", "book_id": "1", "event_ts": "no-es-fecha"},
        {**base, "user_id": "7", "book_id": "1", "progress_pct": "150"},
    ])


@pytest.fixture(scope="module")
def result(tmp_path_factory):
    d = tmp_path_factory.mktemp("edge")
    good = build_events()
    for c in ("event_ts", "ingested_at"):
        good[c] = good[c].dt.strftime("%Y-%m-%d %H:%M:%S")
    good["progress_pct"] = good["progress_pct"].map(lambda v: "" if pd.isna(v) else str(int(v)))
    logs = pd.concat([good.astype(str), dirty_rows()], ignore_index=True)
    logs = logs.sample(frac=1, random_state=1)
    logs.to_csv(d / "logs.csv", index=False)
    pd.DataFrame({
        "book_id": ["1", "2", "3", "4", "4", ""],
        "title": ["A", "B", "C", "D", "D-duplicado", "sin id"],
        "author": ["a"] * 6, "genre": ["g"] * 6, "language": ["es"] * 6,
        "total_pages": ["100", "", "100", "100", "100", "1"],
        "published_year": ["2000"] * 6,
    }).to_csv(d / "catalog.csv", index=False)
    report = run(d / "logs.csv", d / "catalog.csv", d / "out", reference_ts=REF)
    outs = {n: pd.read_parquet(d / "out" / f"{n}.parquet") for n in [
        "reading_time_by_user_book", "dropoff_by_user_book", "dropoff_by_book",
        "binge_by_user_book", "binge_by_book"]}
    return report, outs


def pair(df, uid):
    r = df[df.user_id == uid]
    assert len(r) == 1, f"usuario {uid}: {len(r)} filas"
    return r.iloc[0]


def users(df):
    return set(df.user_id)


def test_dirty_rows_are_counted_and_skipped(result):
    report, o = result
    assert report["invalid_key"] == 5
    assert report["invalid_timestamp"] == 1
    assert report["invalid_progress"] == 1
    assert report["missing_progress"] == 1
    assert 7 not in users(o["reading_time_by_user_book"])


def test_catalog_duplicates_and_null_ids_do_not_duplicate_outputs(result):
    report, o = result
    assert report["catalog_duplicate_id"] == 1
    assert report["catalog_invalid_id"] == 1
    for df in o.values():
        key = ["user_id", "book_id"] if "user_id" in df else ["book_id"]
        assert not df.duplicated(key).any()
    assert o["binge_by_book"].set_index("book_id").loc[4, "title"] == "D"


def test_session_gap_boundary_is_strictly_greater_than_30_min(result):
    _, o = result
    rt = o["reading_time_by_user_book"]
    assert pair(rt, 101).n_sessions == 1 and pair(rt, 101).reading_minutes == pytest.approx(30)
    assert pair(rt, 102).n_sessions == 2 and pair(rt, 102).reading_minutes == 0


def test_close_ends_reading_interval_even_within_30_min(result):
    _, o = result
    r = pair(o["reading_time_by_user_book"], 103)
    assert r.n_sessions == 1
    assert r.reading_minutes == pytest.approx(20)  # los 10 min entre close y open no cuentan


def test_open_without_progress_keeps_session_start(result):
    _, o = result
    r = pair(o["reading_time_by_user_book"], 206)
    assert r.n_events == 2 and r.reading_minutes == pytest.approx(10)
    assert r.start_progress_pct == 10  # se toma el siguiente progreso conocido


def test_dropoff_range_and_inactivity_bounds_are_inclusive(result):
    _, o = result
    flagged = users(o["dropoff_by_user_book"])
    assert {111, 112, 115} <= flagged
    assert not {113, 114, 116} & flagged


def test_binge_span_boundary_24h_inclusive(result):
    _, o = result
    b = users(o["binge_by_user_book"])
    assert 121 in b and 122 not in b


def test_binge_with_short_pauses_and_close_reopen(result):
    _, o = result
    r = pair(o["binge_by_user_book"], 201)
    assert r.sessions_to_finish == 1
    # 3 bloques de 60 min de lectura; las pausas de 10 y 25 min no cuentan.
    assert r.reading_hours == pytest.approx(3.0, abs=0.01)


def test_binge_requires_real_coverage(result):
    _, o = result
    b = users(o["binge_by_user_book"])
    assert 203 not in b  # finish aislado: 0 h de lectura
    assert 204 not in b  # arranca en 85%


def test_binge_survives_reopening_after_finish(result):
    _, o = result
    r = pair(o["binge_by_user_book"], 205)
    assert r.sessions_to_finish == 1
    assert r.hours_to_finish == pytest.approx(3.0, abs=0.01)
