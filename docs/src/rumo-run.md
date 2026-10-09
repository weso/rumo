# Run the rules

Use the `run` method:

```python
results = rumo.run(params={"GOOD_SCORE": 90}, predicates=["good"])
```

`run` returns a `RumoResults` object.
Refer to [Use the results](rumo-results.md).

| Argument | Default | Description |
|----------|---------|-------------|
| `params` | `None` | A dictionary with values for the global parameters of the rules. |
| `predicates` | `None` | A list of the predicates to return. |

## Parameters

A rule program can use global parameters, for example `$GOOD_SCORE`.
Give the values in the `params` dictionary.
Do not write the `$` character in the key.

```python
rumo.run(params={"GOOD_SCORE": 90, "CITY": "Oviedo"})
```

`Rumo` converts each Python value to a nemo value:

| Python value | nemo value | Example |
|--------------|------------|---------|
| `int` | Integer | `90` → `90` |
| `float` | Number | `80.5` → `80.5` |
| `bool` | Boolean | `True` → `"true"^^xsd:boolean` |
| `str` | String | `"Bob"` → `"Bob"` |
| `str` with the form `<...>` | IRI | `"<http://example.org/a>"` → `<http://example.org/a>` |

Other Python types cause a `ValueError`.

## Select the predicates

If you do not set `predicates`, `run` returns:

1. The predicates in `@output` statements and `@export` statements of the rules.
2. If the rules have no such statements, all predicates that the rules calculate.

To get a different set of predicates, set `predicates`:

```python
results = rumo.run(predicates=["good", "data"])
```

You can select any predicate in the rule program, also an imported predicate.

If a predicate does not exist, `run` raises a `RuntimeError`.

> **Note:** `run` does not write the files of `@export` statements.
> `run` only returns the facts.

## Errors

| Error | Cause |
|-------|-------|
| `ValueError` | There are no rules. A parameter has an incorrect type. |
| `RuntimeError` | The rule program has an error. A predicate does not exist. nemo cannot read the data. |
