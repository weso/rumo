# Installation

## Install the Python package

Use `pip` to install the package:

```sh
pip install pyrumo
```

The package needs Python 3.9 or a later version.
The package also installs Polars.

To use pandas DataFrames, install the `pandas` extra:

```sh
pip install "pyrumo[pandas]"
```

## Install the command-line tool

### Use a prebuilt binary

Each GitHub release has prebuilt binaries for these platforms:

| Operating system | Architecture |
|------------------|--------------|
| Linux | x86_64, aarch64 |
| Windows | x86_64 |
| macOS | x86_64, aarch64 |

To install a binary:

1. Download the archive for your platform from the release page.
2. Extract the archive.
3. Move the `rumo` file to a directory in your `PATH`.

### Build from the source code

The nemo rule engine needs the nightly Rust toolchain.
The file `rust-toolchain.toml` selects this toolchain automatically.

1. Install Rust with [rustup](https://rustup.rs/).
2. Clone the repository.
3. In the repository directory, run this command:

```sh
cargo install --path . --features rules
```

> **Note:** If you do not set the `rules` feature, the `rumo rules` command is not available.

## Cargo features

| Feature | Description |
|---------|-------------|
| `rules` | Adds the nemo rule engine. |
| `python` | Adds the Python bindings. This feature also sets the `rules` feature. |
