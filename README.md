# rumo

`rumo` is a Rust library that reads and describes Polars DataFrames and runs rules on them using nemo. 

## CLI usage

Example of how to run rumo against a CSV file:

```sh
cargo run --features rules -- rules --rules examples/sample.rls --data examples/sample.csv --param GOOD_SCORE=90```

## Python usage

Build and install the Python extension locally:

```sh
python3 -m venv .venv                                                                                                                     
source .venv/bin/activate
pip install polars                                                                                                                        
maturin develop --features python                                                                                                         
python examples/describe_dataframe.py
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

