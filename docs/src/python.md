# Python: describe and convert

The `pyrumo` module has three functions for Polars DataFrames.

```python
import polars as pl
import pyrumo

df = pl.read_csv("examples/sample.csv")
```

## Get the structure of a DataFrame

`describe` returns a text with the number of rows, the columns and the data types:

```python
print(pyrumo.describe(df))
```

```text
DataFrame: 5 rows × 3 columns
  - name (str)
  - age (i64)
  - score (f64)
```

`print_info` writes the same text to the standard output:

```python
pyrumo.print_info(df)
```

## Convert a DataFrame to Turtle

`to_turtle` returns the DataFrame as a Turtle text:

```python
ttl = pyrumo.to_turtle(df, "http://example.org/", "r")
print(ttl)
```

```text
prefix : <http://example.org/>

:r0 :name "Alice" ;
    :age 25 ;
    :score 88.5 .
...
```

The second argument is the prefix IRI.
The third argument is the start of the name of each row.

Refer to [DataFrame to RDF conversion](conversion.md) for the rules of the conversion.

> **Note:** These three functions accept only Polars DataFrames.
> The `Rumo` class accepts pandas DataFrames and Polars DataFrames.
