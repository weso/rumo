# rumo

`rumo` reads tabular data and runs rules on it.
It uses the [nemo](https://github.com/knowsys/nemo) rule engine.

`rumo` has three parts:

- A Rust library.
- A command-line tool with the name `rumo`.
- A Python package with the name `pyrumo`.

With `rumo`, you can:

- Show the structure of a table.
- Convert a table to RDF in the Turtle format.
- Run nemo rules on a pandas DataFrame, a Polars DataFrame or RDF data.
- Get the results as a pandas DataFrame, a Polars DataFrame or a list of dictionaries.

The full manual is in the `docs` directory. Refer to [Documentation](#documentation).

## Installation

### Python

```sh
pip install pyrumo
```

To use pandas DataFrames, install the `pandas` extra:

```sh
pip install "pyrumo[pandas]"
```

### Command-line tool

Each GitHub release has prebuilt binaries for Linux (x86_64, aarch64), Windows (x86_64) and macOS (x86_64, aarch64).

To build from the source code, use this command:

```sh
cargo install --path . --features rules
```

The nemo rule engine needs the nightly Rust toolchain.
The file `rust-toolchain.toml` selects this toolchain automatically.

## Quick start (Python)

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
results = rumo.run(params={"GOOD_SCORE": 90})

print(results.to_pandas("good"))
```

The output is:

```text
   name  score
0   Bob   92.0
1  Dave   95.1
```

## Python usage

### Run rules with the Rumo class

A `Rumo` object keeps one rule program and the data for the rules.

1. Load the rules from a string (`rules=` or `read_rules_str`) or from a file (`rules_file=` or `read_rules_file`).
2. Load the data. The data can be:
   - A pandas DataFrame or a Polars DataFrame (`data=` or `read_dataframe`).
   - RDF data as a string (`data="..."` or `read_data_str`).
   - A data file (`data_file=` or `read_data_file`).
3. Run the rules with `run(params=..., predicates=...)`.
4. Read the results from the `RumoResults` object.

`Rumo` converts each DataFrame to Turtle.
Each row becomes a subject (`:r0`, `:r1`, …).
Each column becomes a property (`:name`, `:age`, …).

`Rumo` keeps the data in memory with a resource name.
The default resource name is `data.ttl`. For a data file, it is the file name.
There are two methods to connect the data to the rules:

- Set `predicate="data"`. `Rumo` then adds `@import data :- turtle { resource = "data.ttl" } .` to the rules.
- Use the resource name in your own `@import` statement. For example, this runs `examples/sample.rls` without changes:

  ```python
  rumo = Rumo(rules_file="examples/sample.rls", data=df, resource="sample.ttl")
  ```

#### Parameters

`params` gives values for the global parameters of the rules, for example `$GOOD_SCORE`:

| Python value | nemo value |
|--------------|------------|
| `int`, `float` | Number |
| `bool` | Boolean |
| `str` | String |
| `str` with the form `"<http://...>"` | IRI |

#### Results

If you do not set `predicates`, `run` returns the `@output` and `@export` predicates of the rules.
If the rules have no such predicates, `run` returns all calculated predicates.

| Method | Returns |
|--------|---------|
| `to_pandas(p)` | A pandas DataFrame. |
| `to_polars(p)` | A Polars DataFrame. |
| `to_dicts(p)` | A list of dictionaries. |
| `rows(p)` | A list of tuples. |
| `to_dict()` | A dictionary with all predicates. |
| `results["p"]` | A list of dictionaries. |

You can omit `p` if the results have only one predicate.

The column names are the variable names in the rule heads.
For example, `good(?name, ?score)` gives the columns `name` and `score`.
To set different names, use `columns=[...]`.

Refer to `examples/run_rules.py` for a full example.

### Describe and convert a Polars DataFrame

```python
import polars as pl
import pyrumo

df = pl.read_csv("examples/sample.csv")
print(pyrumo.describe(df))
print(pyrumo.to_turtle(df, "http://example.org/", "r"))
```

Refer to `examples/describe_dataframe.py` for a full example.

## Command-line usage

Show the structure of a CSV file:

```sh
rumo describe --data examples/sample.csv
```

Convert a CSV file to Turtle:

```sh
rumo convert --data examples/sample.csv --result-format turtle
```

Run a rule file:

```sh
rumo rules --rules examples/sample.rls --data examples/sample.csv --param GOOD_SCORE=90
```

## Examples

| File | Description |
|------|-------------|
| `examples/sample.csv` | A sample CSV file. |
| `examples/sample.ttl` | The sample CSV file as Turtle. |
| `examples/sample.rls` | A sample nemo rule file. |
| `examples/describe_dataframe.py` | Shows the structure of a DataFrame in Python. |
| `examples/run_rules.py` | Runs rules on a pandas DataFrame in Python. |

## Documentation

The manual uses [mdBook](https://rust-lang.github.io/mdBook/).
The source files are in `docs/src`.

To read the manual:

1. Install mdBook: `cargo install mdbook`.
2. Build and open the manual: `mdbook serve docs --open`.

The `Docs` workflow publishes the manual to GitHub Pages when `docs/` changes on the main branch.
Before the first publication, set **Settings → Pages → Source** to **GitHub Actions**.

The manual is written in Simplified Technical English.

## Development

To build and test the Python extension:

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install maturin
maturin develop --extras test
pytest
```

To run the Rust tests:

```sh
cargo test
cargo test --features rules
```

## Releasing

1. Change `version` in `Cargo.toml`. This is also the version of the Python package.
2. Publish a GitHub release. The tag must be the version, for example `v0.2.0`.

The `Binaries` workflow attaches the command-line archives to the release.
The `Python` workflow publishes the `pyrumo` wheels to PyPI.

## License

MIT
