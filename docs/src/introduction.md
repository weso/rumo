# Introduction

`rumo` is a tool that reads tabular data and runs rules on it.
It uses the [nemo](https://github.com/knowsys/nemo) rule engine.

`rumo` has three parts:

- A Rust library.
- A command-line tool with the name `rumo`.
- A Python package with the name `pyrumo`.

## What rumo does

`rumo` can do these tasks:

1. Show the structure of a table (the number of rows, the columns and the data types).
2. Convert a table to RDF in the Turtle format.
3. Run nemo rules on a table or on RDF data.
4. Return the results of the rules as Python data, a pandas DataFrame or a Polars DataFrame.

## How rumo runs rules

The diagram shows the data flow:

```text
 pandas / Polars DataFrame ──► Turtle (RDF) ──┐
                                              ├──► nemo rules ──► results
 RDF text or RDF file ────────────────────────┘
```

`rumo` converts each DataFrame to RDF triples.
The nemo rules then read the triples and calculate new facts.
`rumo` returns these facts as tables.

## Terms in this manual

| Term | Meaning |
|------|---------|
| Rule program | A text in the nemo rule language. A rule file has the extension `.rls`. |
| Predicate | The name of a relation in a rule program, for example `good`. |
| Fact | One row of a predicate, for example `good("Bob", 92.0)`. |
| Resource | A name that a rule program uses in an `@import` statement to read data. |
| Parameter | A global value in a rule program, for example `$GOOD_SCORE`. |

## Example

```python
import pandas as pd
from pyrumo import Rumo

df = pd.read_csv("examples/sample.csv")

rules = """
@prefix : <http://example.org/> .
good(?name, ?score) :- data(?r, :name, ?name), data(?r, :score, ?s),
    ?score = DOUBLE(?s), ?score > $GOOD_SCORE .
"""

rumo = Rumo(rules=rules, data=df, predicate="data")
print(rumo.run(params={"GOOD_SCORE": 90}).to_pandas("good"))
```

The output is:

```text
   name  score
0   Bob   92.0
1  Dave   95.1
```
