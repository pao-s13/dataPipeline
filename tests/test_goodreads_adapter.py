"""Prueba de integración del adaptador de Goodreads + pipeline con el preset `sparse`.

Usa archivos .json.gz chicos con el mismo formato que los del UCSD Book Graph.
"""
import gzip
import json

import pandas as pd
import pytest

from goodreads_adapter import adapt
from reading_pipeline.config import PipelineConfig
from reading_pipeline.run import run


def gr(ts: str) -> str:
    """'2017-03-01 10:00:00' -> formato de Goodreads con offset."""
    return pd.Timestamp(ts).strftime("%a %b %d %H:%M:%S -0800 %Y")


def inter(user, book, started="", read="", is_read=True, updated="2017-11-01 00:00:00"):
    return {"user_id": user, "book_id": book, "review_id": f"{user}{book}", "is_read": is_read,
            "rating": 4, "review_text_incomplete": "", "date_added": gr(updated),
            "date_updated": gr(updated), "read_at": gr(read) if read else "",
            "started_at": gr(started) if started else ""}


def write_gz(path, rows):
    with gzip.open(path, "wt") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


@pytest.fixture(scope="module")
def result(tmp_path_factory):
    raw = tmp_path_factory.mktemp("raw")
    interactions = [
        inter("aa", "1", "2017-03-01 20:00:00", "2017-03-01 23:30:00"),  # binge (3.5 h)
        inter("bb", "1", "2017-03-01 00:00:00", "2017-03-01 00:00:00"),  # binge, solo fecha
        inter("cc", "1", "2017-03-01 10:00:00", "2017-03-20 10:00:00"),  # 19 días, no binge
        inter("dd", "2", "2017-01-05 10:00:00"),                         # empezó y no terminó
        inter("ee", "2", "2017-12-20 10:00:00"),                         # empezó hace poco
        inter("ff", "2", read="2017-02-01 00:00:00"),                    # solo fin
        inter("gg", "2", "2017-02-01 10:00:00", is_read=False),          # read_at sin is_read
        inter("hh", "2", read="1995-06-01 00:00:00"),                    # fuera de 2007-2017
        inter("ii", "2", is_read=False),                                 # solo en estante
    ]
    write_gz(raw / "goodreads_interactions_test.json.gz", interactions)
    review = {k: v for k, v in interactions[0].items() if k != "is_read"}
    review["review_text"] = "great"
    write_gz(raw / "goodreads_reviews_test.json.gz", [review])  # repite las fechas de aa/1
    write_gz(raw / "goodreads_books_test.json.gz", [
        {"book_id": b, "title": t, "authors": [{"author_id": "9", "role": ""}],
         "language_code": "eng", "num_pages": "100", "publication_year": "2015",
         "popular_shelves": [{"count": "5", "name": "to-read"}, {"count": "3", "name": "poetry"}]}
        for b, t in [("1", "Libro corto"), ("2", "Libro largo")]])

    out = tmp_path_factory.mktemp("adapted")
    stats = adapt(raw, "test", out)
    report = run(out / "reading_logs.csv", out / "catalog.csv", out / "pipeline",
                 PipelineConfig.sparse_events(), reference_ts="2017-12-31")
    users = pd.read_csv(out / "user_id_map.csv").set_index("goodreads_user_id")["user_id"]
    read = lambda n: pd.read_parquet(out / "pipeline" / f"{n}.parquet")
    return stats, report, users, read


def ids(df, users, *names):
    return set(df.user_id) == {users[n] for n in names}


def test_adapter_maps_dates_to_events(result):
    stats, _, _, _ = result
    # aa, bb, cc: open + finish; dd, ee, gg: open; ff: finish; reseña de aa: open + finish.
    assert stats["events_open"] == 7 and stats["events_finish"] == 5
    assert stats["events_outside_2007_2017_dropped"] == 1


def test_reviews_duplicate_interactions_and_are_removed(result):
    _, report, _, _ = result
    assert report["duplicates_removed"] == 2


def test_binge_uses_span_without_reading_time(result):
    _, _, users, read = result
    assert ids(read("binge_by_user_book"), users, "aa", "bb")


def test_dropoff_proxy_is_started_never_finished_and_inactive(result):
    _, _, users, read = result
    assert ids(read("dropoff_by_user_book"), users, "dd", "gg")


def test_catalog_genre_skips_status_shelves(result):
    _, _, _, read = result
    assert set(read("binge_by_book")["genre"]) == {"poetry"}
