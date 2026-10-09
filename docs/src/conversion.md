# DataFrame to RDF conversion

`rumo` converts a DataFrame to RDF in the Turtle format.
The command-line tool, `pyrumo.to_turtle` and `Rumo` use the same conversion.

## Conversion rules

- Each row becomes a subject. The name of the subject is `<base_url><row_stem><index>`. The first index is 0.
- Each column becomes a property. The name of the property is `<base_url><column name>`.
- Each cell becomes one triple: the row, the column property and the value.

## Values

| DataFrame type | Turtle value | Example |
|----------------|--------------|---------|
| String | Quoted string | `"Alice"` |
| Integer | Number without quotes | `25` |
| Float | Number with a decimal point | `92.0` |
| Null | Empty string | `""` |
| Other types | The Polars text form | `true` |

## Example

This table:

| name | age | score |
|------|-----|-------|
| Alice | 25 | 88.5 |
| Bob | 30 | 92.0 |

gives this Turtle text with `base_url="http://example.org/"` and `row_stem="r"`:

```text
prefix : <http://example.org/>

:r0 :name "Alice" ;
    :age 25 ;
    :score 88.5 .
:r1 :name "Bob" ;
    :age 30 ;
    :score 92.0 .
```

In a rule program, these triples have this form:

```text
data(<http://example.org/r0>, <http://example.org/name>, "Alice")
```

Use a `@prefix` statement in the rules to write the IRIs in a short form:

```text
@prefix : <http://example.org/> .
young(?name) :- data(?r, :name, ?name), data(?r, :age, ?age), ?age < 28 .
```

## Limitations

Refer to [Limitations and troubleshooting](troubleshooting.md).
