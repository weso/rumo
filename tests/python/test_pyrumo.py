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


# ── Rumo ──────────────────────────────────────────────────────────────────────

import os

import pytest
from pyrumo import Rumo

EXAMPLES = os.path.join(os.path.dirname(__file__), "..", "..", "examples")

RULES = """
@prefix : <http://example.org/> .
good(?name, ?score) :- data(?r, :name, ?name), data(?r, :score, ?s),
    ?score = DOUBLE(?s), ?score > $MIN .
"""


def test_rumo_polars_dataframe_with_predicate():
    results = Rumo(rules=RULES, data=sample(), predicate="data").run(params={"MIN": 90})
    assert results.predicates() == ["good"]
    assert results.columns() == ["name", "score"]
    assert results.to_dicts() == [{"name": "Bob", "score": 92.0}]


def test_rumo_pandas_dataframe():
    pd = pytest.importorskip("pandas")
    df = pd.DataFrame({"name": ["Alice", "Bob"], "score": [88.5, 92.0]})
    results = Rumo(rules=RULES, data=df, predicate="data").run(params={"MIN": 80})
    out = results.to_pandas("good").sort_values("name").reset_index(drop=True)
    assert list(out.columns) == ["name", "score"]
    assert out["name"].tolist() == ["Alice", "Bob"]
    assert out["score"].tolist() == [88.5, 92.0]


def test_rumo_rules_file_imports_resource():
    rumo = Rumo(
        rules_file=os.path.join(EXAMPLES, "sample.rls"),
        data=pl.read_csv(os.path.join(EXAMPLES, "sample.csv")),
        resource="sample.ttl",
    )
    rows = sorted(rumo.run(params={"GOOD_SCORE": 90}).rows("good"))
    assert rows == [("Bob", 92.0), ("Dave", 95.1)]


def test_rumo_rdf_string_and_methods():
    rumo = Rumo()
    rumo.read_rules_str('@prefix : <http://example.org/> .\nnamed(?r) :- data(?r, :name, $WHO) .')
    rumo.read_data_str('@prefix : <http://example.org/> .\n:a :name "Alice" .\n:b :name "Bob" .',
                       predicate="data")
    results = rumo.run(params={"WHO": "Bob"})
    assert results["named"] == [{"r": "http://example.org/b"}]
    assert "named" in results and len(results) == 1
    assert results.to_dict() == {"named": [{"r": "http://example.org/b"}]}

    rumo.reset_data()
    assert rumo.run(params={"WHO": "Bob"}).rows("named") == []


def test_rumo_data_file_and_explicit_predicates():
    rumo = Rumo(
        rules='@prefix : <http://example.org/> .\nn(?n) :- data(?r, :name, ?n) .',
        data_file=os.path.join(EXAMPLES, "sample.ttl"),
        predicate="data",
    )
    assert rumo.resources == ["sample.ttl"]
    results = rumo.run(predicates=["n"])
    assert sorted(r["n"] for r in results.to_dicts()) == ["Alice", "Bob", "Carol", "Dave", "Eve"]
    assert results.to_polars(columns=["who"]).columns == ["who"]


def test_rumo_errors():
    with pytest.raises(ValueError, match="no rules"):
        Rumo(data=sample()).run()
    with pytest.raises(RuntimeError, match="expected"):
        Rumo(rules="p(?x :- .").run()
    rumo = Rumo(rules=RULES, data=sample(), predicate="data")
    with pytest.raises(RuntimeError, match="Unknown predicate"):
        rumo.run(params={"MIN": 1}, predicates=["missing"])
    with pytest.raises(KeyError):
        rumo.run(params={"MIN": 1})["missing"]
    with pytest.raises(ValueError, match="columns"):
        rumo.run(params={"MIN": 1}).to_dicts("good", columns=["only_one"])
