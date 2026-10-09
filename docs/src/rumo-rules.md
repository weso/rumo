# Load rules

A `Rumo` object keeps one rule program.
When you load a new rule program, it replaces the old rule program.

## Load rules from a string

```python
rumo.read_rules_str("""
@prefix : <http://example.org/> .
adult(?name) :- data(?p, :name, ?name), data(?p, :age, ?age), ?age >= 18 .
""")
```

You can also use the `rules` argument of the constructor.

## Load rules from a file

```python
rumo.read_rules_file("examples/sample.rls")
```

You can also use the `rules_file` argument of the constructor.

`Rumo` reads the file immediately.
If you change the file later, load it again.

## Paths in @import statements

A rule program can read files with `@import` statements.
`Rumo` finds these files in this sequence:

1. The data resources of the `Rumo` object. Refer to [Load data](rumo-data.md).
2. Files relative to a directory:
   - For a rule file, this is the directory of the rule file.
   - For a rule string, this is the current working directory.

## Remove the rules

```python
rumo.reset_rules()
```

If you run a `Rumo` object without rules, it raises a `ValueError`.

## Errors in the rules

If the rule program has an error, `run` raises a `RuntimeError`.
The message shows the line and the column of the error:

```text
[01] Error: expected `)`
   ╭─[ :1:8 ]
   │
 1 │ foo(?x :- .
   │        │
   │        ╰─ expected `)`
───╯
```
