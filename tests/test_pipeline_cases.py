"""Pruebas de integración con casos de negocio conocidos (CA 1 a CA 4).

Se arma un CSV pequeño donde sabemos de antemano el resultado correcto, se corre
el pipeline completo (carga optimizada -> Parquet) y se validan las salidas.
"""
import pandas as pd
import pytest

from reading_pipeline.run import run

REF = pd.Timestamp("2026-06-30 00:00:00")
T0 = pd.Timestamp("2026-03-01 10:00:00")


def ev(uid, bid, etype, ts, pct, eid=None, ingest_delay_s=30):
    return {
        "event_id": eid or f"{uid}-{bid}-{ts:%Y%m%d%H%M%S}-{etype}",
        "user_id": uid, "book_id": bid, "event_type": etype,
        "event_ts": ts, "progress_pct": pct, "device": "ios", "app_version": "4.0.0",
        "ingested_at": ts + pd.Timedelta(seconds=ingest_delay_s),
    }


def m(minutes):
    return pd.Timedelta(minutes=minutes)


def build_events():
    rows = []
    # u1/b10: dos sesiones (10 min + 10 min) separadas por 2 h, con reintentos.
    s1 = [ev(1, 10, "open", T0, 0), ev(1, 10, "progress", T0 + m(5), 5),
          ev(1, 10, "close", T0 + m(10), 10)]
    s2_start = T0 + pd.Timedelta(hours=2)
    s2 = [ev(1, 10, "open", s2_start, 10), ev(1, 10, "close", s2_start + m(10), 20)]
    rows += s1 + s2
    # Reintento con el mismo event_id y reintento con event_id nuevo.
    rows.append({**s1[1], "ingested_at": s1[1]["ingested_at"] + pd.Timedelta(hours=1)})
    rows.append({**s2[1], "event_id": "retry-new-id",
                 "ingested_at": s2[1]["ingested_at"] + pd.Timedelta(hours=3)})

    # u2/b10: abandono en 50%, inactivo hace más de 30 días -> drop-off.
    old = REF - pd.Timedelta(days=60)
    rows += [ev(2, 10, "open", old, 0), ev(2, 10, "progress", old + m(15), 30),
             ev(2, 10, "close", old + m(25), 50)]
    # u3/b10: 50% pero con actividad reciente -> en curso, no drop-off.
    recent = REF - pd.Timedelta(days=5)
    rows += [ev(3, 10, "open", recent, 0), ev(3, 10, "close", recent + m(20), 50)]
    # u4/b20: abandono en 30% (fuera de rango) -> no drop-off.
    rows += [ev(4, 20, "open", old, 0), ev(4, 20, "close", old + m(20), 30)]

    # u5/b20: termina en una sola sesión de ~3 h -> binge.
    rows.append(ev(5, 20, "open", T0, 0))
    rows += [ev(5, 20, "progress", T0 + m(10 * k), 5 * k) for k in range(1, 19)]
    rows.append(ev(5, 20, "finish", T0 + m(190), 100))
    # u6/b20: termina en 3 sesiones a lo largo de 5 días -> no binge.
    for day, (a, b) in enumerate([(0, 40), (40, 70), (70, 100)]):
        t = T0 + pd.Timedelta(days=2 * day + 0.1)
        rows += [ev(6, 20, "open", t, a),
                 ev(6, 20, "finish" if b == 100 else "close", t + m(25), b)]
    # u7/b30: termina en 2 sesiones dentro de 20 h -> binge por lapso corto.
    rows += [ev(7, 30, "open", T0, 0), ev(7, 30, "close", T0 + m(25), 50),
             ev(7, 30, "open", T0 + pd.Timedelta(hours=19), 50),
             ev(7, 30, "finish", T0 + pd.Timedelta(hours=19) + m(25), 100)]

    # Evento inválido (progreso fuera de rango) -> se descarta.
    rows.append(ev(8, 30, "progress", T0, 150))
    return pd.DataFrame(rows)


@pytest.fixture(scope="module")
def outputs(tmp_path_factory):
    d = tmp_path_factory.mktemp("cases")
    logs = build_events().sample(frac=1, random_state=0)  # llegada desordenada
    logs.to_csv(d / "logs.csv", index=False, date_format="%Y-%m-%d %H:%M:%S")
    pd.DataFrame({
        "book_id": [10, 20, 30], "title": ["A", "B", "C"], "author": ["x", "y", "z"],
        "genre": ["romance", "thriller", "ensayo"], "language": ["es", "es", "en"],
        "total_pages": [200, 300, 150], "published_year": [2001, 2010, 2020],
    }).to_csv(d / "catalog.csv", index=False)
    report = run(d / "logs.csv", d / "catalog.csv", d / "out", reference_ts=REF)
    read = lambda n: pd.read_parquet(d / "out" / f"{n}.parquet")
    return report, {n: read(n) for n in [
        "reading_time_by_user_book", "dropoff_by_user_book", "dropoff_by_book",
        "binge_by_user_book", "binge_by_book"]}


def pair(df, u, b):
    r = df[(df.user_id == u) & (df.book_id == b)]
    assert len(r) == 1
    return r.iloc[0]


def test_ca1_dedup_removes_retries(outputs):
    report, _ = outputs
    assert report["duplicates_removed"] == 2
    assert report["invalid_rows"] == 1


def test_ca2_reading_time_groups_fragmented_sessions(outputs):
    _, o = outputs
    r = pair(o["reading_time_by_user_book"], 1, 10)
    assert r.n_sessions == 2
    assert r.reading_minutes == pytest.approx(20.0)  # la pausa de 2 h no cuenta
    assert r.reading_hours == pytest.approx(20 / 60, abs=1e-3)


def test_ca3_dropoff_only_in_range_and_inactive(outputs):
    _, o = outputs
    d = o["dropoff_by_user_book"]
    assert set(zip(d.user_id, d.book_id)) == {(2, 10)}
    b10 = o["dropoff_by_book"].set_index("book_id").loc[10]
    assert b10.dropoffs == 1 and b10.readers == 3
    assert b10.dropoff_rate == pytest.approx(1 / 3)
    assert b10.title == "A"


def test_ca4_binge_single_session_or_short_span(outputs):
    _, o = outputs
    b = o["binge_by_user_book"]
    assert set(zip(b.user_id, b.book_id)) == {(5, 20), (7, 30)}
    assert pair(b, 5, 20).sessions_to_finish == 1
    by_book = o["binge_by_book"].set_index("book_id")
    assert by_book.loc[20].completions == 2 and by_book.loc[20].binge_completions == 1


def test_outputs_have_no_duplicate_keys(outputs):
    _, o = outputs
    for name in ["reading_time_by_user_book", "dropoff_by_user_book", "binge_by_user_book"]:
        assert not o[name].duplicated(["user_id", "book_id"]).any()
