"""
Example: run nemo rules over a pandas DataFrame with a Rumo object.

Prerequisites:
    maturin develop
    pip install pandas
"""
import pandas as pd
from pyrumo import Rumo

df = pd.DataFrame({
    "name":  ["Alice", "Bob", "Carol", "Dave"],
    "age":   [25, 30, 22, 35],
    "score": [88.5, 92.0, 76.3, 95.1],
})

# Each row becomes :r0, :r1, … with one property per column, and
# `predicate="data"` imports those triples as data(?row, ?property, ?value).
rules = """
@prefix : <http://example.org/> .

good(?name, ?score) :- data(?r, :name, ?name), data(?r, :score, ?s),
    ?score = DOUBLE(?s), ?score > $GOOD_SCORE .
"""

rumo = Rumo(rules=rules, data=df, predicate="data")
results = rumo.run(params={"GOOD_SCORE": 90})

print(results.to_pandas("good"))
print(results.to_dicts("good"))

# The same with a rule file that imports the data itself
# (`@import r :- turtle { resource = "sample.ttl" }`).
rumo = Rumo(rules_file="examples/sample.rls", data=df, resource="sample.ttl")
print(rumo.run(params={"GOOD_SCORE": 90}).to_pandas())
