# Development

## Repository structure

| Path | Contents |
|------|----------|
| `src/lib.rs` | The DataFrame structure and the Turtle conversion. |
| `src/rules.rs` | The nemo rule runner. |
| `src/python.rs` | The Python bindings (`pyrumo`). |
| `src/main.rs` | The command-line tool. |
| `tests/` | The Rust tests and the Python tests. |
| `examples/` | Example data, rules and Python scripts. |
| `docs/` | This manual. |

## Run the Rust tests

```sh
cargo test
cargo test --features rules
```

## Build and test the Python package

1. Make a virtual environment:

   ```sh
   python3 -m venv .venv
   source .venv/bin/activate
   ```

2. Install maturin:

   ```sh
   pip install maturin
   ```

3. Build and install the package:

   ```sh
   maturin develop --extras test
   ```

4. Run the tests:

   ```sh
   pytest
   ```

## Build this manual

The manual uses [mdBook](https://rust-lang.github.io/mdBook/).

1. Install mdBook:

   ```sh
   cargo install mdbook
   ```

2. Build the manual:

   ```sh
   mdbook build docs
   ```

   The HTML files are in `docs/book`.

3. To see the manual while you edit it, run:

   ```sh
   mdbook serve docs --open
   ```

## Writing style

Write the documentation in Simplified Technical English (ASD-STE100):

- Use short sentences. Write a maximum of 20 words in a procedure and 25 words in a description.
- Write one instruction in each sentence.
- Use the active voice.
- Use the imperative form in procedures.
- Use the same word for the same thing.

## Make a release

1. Change `version` in `Cargo.toml`. This is also the version of the Python package.
2. Publish a GitHub release. The tag must be the version, for example `v0.2.0`.

The `Binaries` workflow attaches the command-line archives to the release.
The `Python` workflow publishes the `pyrumo` wheels to PyPI.
