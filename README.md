# rumo

`rumo` is a Rust library that reads and describes Polars DataFrames and runs rules on them using [nemo](https://github.com/knowsys/nemo).

## Installation

### CLI

Prebuilt binaries for Linux (x86_64, aarch64), Windows (x86_64) and macOS (x86_64, aarch64) are attached to each GitHub release.

To build from source (requires the nightly Rust toolchain, selected automatically by `rust-toolchain.toml`):

```sh
cargo install --path . --features rules
```

### Python

```sh
pip install pyrumo
```

## CLI usage

Run rules against a CSV file:

```sh
cargo run --features rules -- rules --rules examples/sample.rls --data examples/sample.csv --param GOOD_SCORE=90
```

Describe a CSV file:

```sh
rumo describe --data examples/sample.csv
```

Convert a CSV file to Turtle:

```sh
rumo convert --data examples/sample.csv --result-format turtle
```

## Python usage

```python
import polars as pl
import pyrumo

df = pl.read_csv("examples/sample.csv")
print(pyrumo.describe(df))
print(pyrumo.to_turtle(df, "http://example.org/", "r"))
```

See `examples/describe_dataframe.py` for a runnable example.

To build and install the Python extension locally:

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install maturin
maturin develop --extras test
pytest
```

## Examples

| File | Description |
|------|-------------|
| `examples/sample.csv` | Sample CSV for the CLI |
| `examples/sample.rls` | Sample nemo rule file |
| `examples/describe_dataframe.py` | Describe a DataFrame from Python |

## Releasing

1. Bump `version` in `Cargo.toml` (it is also the Python package version).
2. Publish a GitHub release whose tag is that version (e.g. `v0.2.0`).

The `Binaries` workflow attaches the CLI archives to the release and the `Python` workflow publishes the `pyrumo` wheels to PyPI.
