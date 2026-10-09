# Command-line tool

The `rumo` command has three subcommands: `describe`, `convert` and `rules`.

To show the help, run this command:

```sh
rumo --help
```

## Show the structure of a CSV file

Use the `describe` subcommand:

```sh
rumo describe --data examples/sample.csv
```

The output is:

```text
DataFrame: 5 rows × 3 columns
  - name (str)
  - age (i64)
  - score (f64)
```

| Option | Description |
|--------|-------------|
| `--data <PATH>` | The input file. This option is necessary. |
| `--format <FORMAT>` | The input format. The default is `csv`. Only `csv` is available. |

## Convert a CSV file to Turtle

Use the `convert` subcommand:

```sh
rumo convert --data examples/sample.csv --result-format turtle
```

| Option | Description |
|--------|-------------|
| `--data <PATH>` | The input file. This option is necessary. |
| `--format <FORMAT>` | The input format. The default is `csv`. |
| `--result-format <FORMAT>` | The output format. This option is necessary. Only `turtle` is available. |
| `--output <PATH>` | The output file. If you do not set this option, `rumo` writes to the standard output. |
| `--base-url <IRI>` | The prefix IRI. The default is `http://example.org/`. |
| `--stem <TEXT>` | The start of the name of each row. The default is `r`. |

Refer to [DataFrame to RDF conversion](conversion.md) for the output format.

## Run a rule file

Use the `rules` subcommand:

```sh
rumo rules --rules examples/sample.rls --data examples/sample.csv --param GOOD_SCORE=90
```

| Option | Description |
|--------|-------------|
| `--rules <PATH>` | The nemo rule file. This option is necessary. |
| `--data <PATH>` | A data file. `rumo` uses the directory of this file to find the files in `@import` statements. |
| `--output <PATH>` | `rumo` writes the `@export` files to the directory of this path. |
| `--param <KEY=VALUE>` | A value for a global parameter. You can use this option more than one time. |

If you do not set `--data`, `rumo` finds the `@import` files in the directory of the rule file.

If you do not set `--output`, `rumo` writes the contents of all `@export` files to the standard output.

> **Note:** The `rules` subcommand reads the files that the `@import` statements name.
> It does not convert the `--data` file.
> To run rules directly on a DataFrame, use the Python `Rumo` class.
