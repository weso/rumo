# Limitations and troubleshooting

## Limitations

### DataFrame conversion

The DataFrame conversion has these limitations:

- `rumo` does not escape quotes or backslashes in strings.
  A string with a `"` character gives Turtle that is not correct.
- `rumo` writes column names directly as property names.
  A column name with a space or a special character gives Turtle that is not correct.
- `rumo` writes a null value as an empty string `""`.
  Thus, a rule finds a value for each cell, also for a cell with no data.

To prevent these problems, clean the DataFrame before you load it.
For example, rename the columns and remove the null values.

### Other limitations

- `Rumo.run` keeps the Python global interpreter lock (GIL) during the run.
  Other Python threads stop until the run is complete.
- `Rumo.run` does not write the files of `@export` statements.
  To write these files, use the command-line tool.
- The `describe`, `print_info` and `to_turtle` functions accept only Polars DataFrames.

## Troubleshooting

| Problem | Possible cause | Action |
|---------|----------------|--------|
| A predicate has no facts. | The rules do not find the data. | Make sure that the resource name in `@import` is the same as the `resource` argument. |
| A predicate has no facts. | The IRIs in the rules are different from the IRIs in the data. | Make sure that the `@prefix` of the rules is the same as `base_url`. |
| A comparison with a number fails. | The value is an `xsd:decimal`. | Use `DOUBLE(?x)` in the rule. |
| `RuntimeError: Unknown predicate` | The predicate name in `predicates` is not in the rules. | Correct the predicate name. |
| `ValueError: no rules loaded` | You did not load a rule program. | Use `rules`, `rules_file`, `read_rules_str` or `read_rules_file`. |
| `ValueError: results contain several predicates` | You did not give a predicate name. | Give the predicate name, for example `to_pandas("good")`. |
| `ImportError` from `to_pandas` | pandas is not installed. | Install pandas: `pip install pandas`. |
| `rumo rules` shows "Rule support is not enabled". | The binary does not have the `rules` feature. | Build again with `--features rules`. |
