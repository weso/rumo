import polars as pl
import pyrumo


def sample() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "name": ["Alice", "Bob"],
            "age": [25, 30],
            "score": [88.5, 92.0],
        }
    )


def test_describe():
    out = pyrumo.describe(sample())
    assert "2 rows" in out
    assert "3 columns" in out
    assert "name (str)" in out
    assert "score (f64)" in out


def test_print_info(capfd):
    pyrumo.print_info(sample())
    assert "2 rows" in capfd.readouterr().out


def test_to_turtle():
    ttl = pyrumo.to_turtle(sample(), "http://example.org/", "r")
    assert ttl.startswith("prefix : <http://example.org/>")
    assert ':r0 :name "Alice" ;' in ttl
    assert "    :age 25 ;" in ttl
    assert "    :score 92.0 ." in ttl
