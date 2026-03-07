# rumo

`rumo` is a Rust library that reads and describes Polars DataFrames. It ships with a CLI binary and Python bindings.

## CLI usage

Run against any CSV file:

```sh
cargo run --bin rumo -- --file examples/sample.csv
```

Example output:

```
DataFrame: 5 rows × 3 columns
  - name (str)
  - age (i64)
  - score (f64)
```

## Python usage

Build and install the Python extension locally:

```sh
maturin develop --features python
```

Then use it from Python:

```python
import polars as pl
import rumo

df = pl.read_csv("examples/sample.csv")
print(rumo.describe(df))
```

See `examples/describe_dataframe.py` for a runnable example.

## Examples

| File | Description |
|------|-------------|
| `examples/sample.csv` | Sample CSV for the CLI |
| `examples/describe_dataframe.py` | Describe a DataFrame from Python |

