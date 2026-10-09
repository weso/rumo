# Load data

`Rumo` keeps the data in memory.
`Rumo` does not write temporary files.

Each data item has a **resource name**.
The rule program uses this name to read the data.

## Load a DataFrame

```python
rumo.read_dataframe(df)
```

The DataFrame can be a pandas DataFrame or a Polars DataFrame.
`Rumo` converts the DataFrame to Turtle.
Refer to [DataFrame to RDF conversion](conversion.md).

| Argument | Default | Description |
|----------|---------|-------------|
| `df` | | The pandas DataFrame or Polars DataFrame. |
| `resource` | `"data.ttl"` | The resource name. |
| `predicate` | `None` | The predicate for the data. Refer to [Connect the data to the rules](#connect-the-data-to-the-rules). |
| `base_url` | `"http://example.org/"` | The prefix IRI. |
| `row_stem` | `"r"` | The start of the name of each row. |

> **Note:** For a pandas DataFrame, `Rumo` uses Polars to convert the data.
> If `pyarrow` is available, `Rumo` uses it. If `pyarrow` is not available, `Rumo` uses Python lists.

## Load data from a string

```python
rumo.read_data_str("""
@prefix : <http://example.org/> .
:a :name "Alice" .
:b :name "Bob" .
""", predicate="data")
```

| Argument | Default | Description |
|----------|---------|-------------|
| `data` | | The data as text. |
| `format` | `"turtle"` | The nemo import format. |
| `resource` | `"data.ttl"` | The resource name. |
| `predicate` | `None` | The predicate for the data. |

## Load data from a file

```python
rumo.read_data_file("examples/sample.ttl", predicate="data")
```

| Argument | Default | Description |
|----------|---------|-------------|
| `path` | | The path of the file. |
| `format` | From the file extension | The nemo import format. |
| `resource` | The file name | The resource name. |
| `predicate` | `None` | The predicate for the data. |

`Rumo` reads the file immediately.

If you do not set `format`, `Rumo` uses the file extension:

| Extension | Format |
|-----------|--------|
| `.nt` | `ntriples` |
| `.nq` | `nquads` |
| `.rdf`, `.xml` | `rdfxml` |
| `.csv` | `csv` |
| `.tsv` | `tsv` |
| `.trig` | `trig` |
| `.json` | `json` |
| Other extensions | `turtle` |

## Connect the data to the rules

There are two methods to connect the data to the rule program.

### Method 1: Set a predicate

Set the `predicate` argument.
`Rumo` then adds an `@import` statement to the rule program.
For example, `predicate="data"` adds this statement:

```text
@import data :- turtle { resource = "data.ttl" } .
```

For RDF data, the predicate has three columns: subject, property and value.
Use it in the rules like this:

```text
@prefix : <http://example.org/> .
named(?row, ?name) :- data(?row, :name, ?name) .
```

Use this method when you write the rules for `Rumo`.

### Method 2: Use the resource name in the rules

Do not set the `predicate` argument.
Set the `resource` argument to the name in the `@import` statement of the rules.

For example, `examples/sample.rls` has this statement:

```text
@import r :- turtle { resource = "sample.ttl" } .
```

To run this file on a DataFrame, set `resource="sample.ttl"`:

```python
rumo = Rumo(rules_file="examples/sample.rls", data=df, resource="sample.ttl")
```

`Rumo` gives the DataFrame to the rules. `Rumo` does not read the file `sample.ttl`.

Use this method when you have rule files that already have `@import` statements.

## Load more than one data item

You can load more than one data item.
Give each data item a different resource name:

```python
rumo.read_dataframe(people, resource="people.ttl", predicate="person")
rumo.read_dataframe(orders, resource="orders.ttl", predicate="order", row_stem="o")
```

> **Caution:** If you load data with a resource name that already exists, the new data replaces the old data.

> **Caution:** Two DataFrames with the same `base_url` and `row_stem` give the same row names (`:r0`, `:r1`, …).
> To keep the rows different, set a different `row_stem` for each DataFrame.

## See the loaded data

The `resources` property gives the resource names:

```python
print(rumo.resources)   # ['people.ttl', 'orders.ttl']
```

## Remove the data

```python
rumo.reset_data()
```
