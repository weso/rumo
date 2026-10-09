# Use the results

`run` returns a `RumoResults` object.
This object has one table for each predicate.

## Column names

`Rumo` uses the variable names in the heads of the rules as column names.
For example, this rule gives the columns `name` and `score`:

```text
good(?name, ?score) :- ...
```

If a column has no variable name, `Rumo` uses `col0`, `col1`, and so on.
This occurs for imported predicates and for constants in rule heads.

If more than one rule calculates the same predicate, `Rumo` uses the first available name for each column.

To set different column names, use the `columns` argument:

```python
results.to_pandas("good", columns=["person", "points"])
```

The number of names must be the same as the number of columns.
If it is not, `Rumo` raises a `ValueError`.

## Get a pandas DataFrame

```python
df = results.to_pandas("good")
```

This method needs pandas.

## Get a Polars DataFrame

```python
df = results.to_polars("good")
```

## Get a list of dictionaries

```python
rows = results.to_dicts("good")
# [{'name': 'Bob', 'score': 92.0}, {'name': 'Dave', 'score': 95.1}]
```

## Get a list of tuples

```python
rows = results.rows("good")
# [('Bob', 92.0), ('Dave', 95.1)]
```

## Get all the results

`to_dict` returns a dictionary with one list of dictionaries for each predicate:

```python
results.to_dict()
# {'good': [{'name': 'Bob', 'score': 92.0}, ...]}
```

## Omit the predicate name

If the results have only one predicate, you can omit the predicate name:

```python
results.to_pandas()
```

If the results have more than one predicate, you must give the name.
If you do not, `Rumo` raises a `ValueError`.

## Use the results as a mapping

A `RumoResults` object operates like a read-only dictionary:

```python
results.predicates()     # ['good']
results.columns("good")  # ['name', 'score']
"good" in results        # True
len(results)             # 1
for predicate in results:
    print(predicate, results[predicate])
```

`results["good"]` returns a list of dictionaries.
If the predicate does not exist, it raises a `KeyError`.

## Value types

`Rumo` converts each nemo value to a Python value:

| nemo value | Python value |
|------------|--------------|
| String | `str` |
| IRI | `str` without `<` and `>` |
| String with a language tag | `str`, for example `"hola"@es` |
| Integer | `int` |
| Double, float | `float` |
| `xsd:decimal` | `float` |
| Boolean | `bool` |
| Other values | `str` in nemo syntax |

> **Note:** Turtle reads numbers such as `88.5` as `xsd:decimal`.
> Thus, numbers from a DataFrame become `float` values in the results.
