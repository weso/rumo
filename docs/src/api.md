# Python API reference

## Module functions

| Function | Returns | Description |
|----------|---------|-------------|
| `describe(df)` | `str` | Gives the structure of a Polars DataFrame. |
| `print_info(df)` | `None` | Writes the structure of a Polars DataFrame to the standard output. |
| `to_turtle(df, base_url, row_stem)` | `str` | Converts a Polars DataFrame to Turtle. |

## Class `Rumo`

```python
Rumo(rules=None, rules_file=None, data=None, data_file=None, *,
     format=None, resource=None, predicate=None,
     base_url="http://example.org/", row_stem="r")
```

### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `read_rules_str(rules)` | `None` | Loads the rule program from a string. |
| `read_rules_file(path)` | `None` | Loads the rule program from a file. |
| `read_dataframe(df, resource=None, predicate=None, base_url="http://example.org/", row_stem="r")` | `None` | Loads a pandas DataFrame or a Polars DataFrame. |
| `read_data_str(data, format="turtle", resource=None, predicate=None)` | `None` | Loads data from a string. |
| `read_data_file(path, format=None, resource=None, predicate=None)` | `None` | Loads data from a file. |
| `reset_rules()` | `None` | Removes the rule program. |
| `reset_data()` | `None` | Removes all data. |
| `run(params=None, predicates=None)` | `RumoResults` | Runs the rules on the data. |

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `resources` | `list[str]` | The resource names of the loaded data. |

### Default resource names

| Method | Default resource name |
|--------|-----------------------|
| `read_dataframe` | `data.ttl` |
| `read_data_str` | `data.ttl` |
| `read_data_file` | The file name, for example `sample.ttl` |

## Class `RumoResults`

In this table, `predicate` is optional if the results have only one predicate.

| Method | Returns | Description |
|--------|---------|-------------|
| `predicates()` | `list[str]` | The names of the predicates. |
| `columns(predicate=None)` | `list[str]` | The column names of a predicate. |
| `rows(predicate=None)` | `list[tuple]` | The facts as tuples. |
| `to_dicts(predicate=None, columns=None)` | `list[dict]` | The facts as dictionaries. |
| `to_pandas(predicate=None, columns=None)` | `pandas.DataFrame` | The facts as a pandas DataFrame. |
| `to_polars(predicate=None, columns=None)` | `polars.DataFrame` | The facts as a Polars DataFrame. |
| `to_dict()` | `dict[str, list[dict]]` | All facts of all predicates. |
| `results[predicate]` | `list[dict]` | The facts as dictionaries. |
| `predicate in results` | `bool` | `True` if the predicate is in the results. |
| `len(results)` | `int` | The number of predicates. |
| `iter(results)` | iterator | The names of the predicates. |

## Exceptions

| Exception | Cause |
|-----------|-------|
| `ValueError` | An argument is not correct, or there are no rules. |
| `KeyError` | A predicate is not in the results. |
| `RuntimeError` | nemo cannot parse or run the rules. |
| `OSError` | `Rumo` cannot read a file. |
| `ImportError` | pandas is not installed and you call `to_pandas`. |
