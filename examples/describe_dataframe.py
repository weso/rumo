"""
Example: describe a Polars DataFrame using rumo.

Prerequisites:
    maturin develop --features python
"""

import polars as pl
import rumo

df = pl.DataFrame({
    "name":  ["Alice", "Bob", "Carol", "Dave"],
    "age":   [25, 30, 22, 35],
    "score": [88.5, 92.0, 76.3, 95.1],
})

print(rumo.describe(df))
