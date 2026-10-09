# Python: run rules with Rumo

The `Rumo` class runs nemo rules on data.
A `Rumo` object keeps:

- One rule program.
- Zero or more data resources.

## Procedure

1. Make a `Rumo` object.
2. Load the rule program.
3. Load the data.
4. Run the rules.
5. Read the results.

You can do steps 1 to 3 in one call:

```python
from pyrumo import Rumo

rumo = Rumo(rules_file="rules.rls", data=df, predicate="data")
```

You can also do each step with a method:

```python
rumo = Rumo()
rumo.read_rules_file("rules.rls")
rumo.read_dataframe(df, predicate="data")
```

Then run the rules and read the results:

```python
results = rumo.run(params={"GOOD_SCORE": 90})
table = results.to_pandas("good")
```

## Use one object more than one time

A `Rumo` object does not change when you run the rules.
You can run the rules again with different parameters:

```python
high = rumo.run(params={"GOOD_SCORE": 90})
low = rumo.run(params={"GOOD_SCORE": 50})
```

You can also replace the rules or the data and run again.

## Constructor arguments

| Argument | Description |
|----------|-------------|
| `rules` | The rule program as a string. |
| `rules_file` | The path of a rule file. |
| `data` | A pandas DataFrame, a Polars DataFrame or RDF data as a string. |
| `data_file` | The path of a data file. |
| `format` | The nemo import format of `data` or `data_file`. Keyword only. |
| `resource` | The resource name of the data. Keyword only. |
| `predicate` | If you set this name, `Rumo` imports the data into this predicate. Keyword only. |
| `base_url` | The prefix IRI for a DataFrame. The default is `http://example.org/`. Keyword only. |
| `row_stem` | The start of the name of each row of a DataFrame. The default is `r`. Keyword only. |

All arguments are optional.

> **Caution:** Do not set `rules` and `rules_file` together.
> Do not set `data` and `data_file` together.
> If you do, `Rumo` raises a `ValueError`.

The next sections give more information about each step.
