use polars::prelude::*;

#[cfg(feature = "rules")]
pub mod rules;

// ── Turtle serialisation ─────────────────────────────────────────────────────

fn format_float(v: f64) -> String {
    let s = format!("{}", v);
    if s.contains('.') || s.contains('e') { s } else { format!("{}.0", s) }
}

fn anyvalue_to_turtle(value: &AnyValue) -> String {
    match value {
        AnyValue::String(s)      => format!("\"{}\"", s),
        AnyValue::StringOwned(s) => format!("\"{}\"", s),
        AnyValue::Float32(v)     => format_float(*v as f64),
        AnyValue::Float64(v)     => format_float(*v),
        AnyValue::Null           => "\"\"".to_string(),
        other                    => format!("{}", other),
    }
}

/// Convert a DataFrame to a Turtle string.
///
/// * `base_url`  – the prefix IRI, e.g. `"http://example.org/"`
/// * `row_stem`  – local name prefix for each row subject, e.g. `"r"`
pub fn dataframe_to_turtle(df: &DataFrame, base_url: &str, row_stem: &str) -> String {
    let mut out = format!("prefix : <{}>\n\n", base_url);
    let columns = df.columns();
    let ncols = columns.len();

    for row_idx in 0..df.height() {
        for (col_idx, series) in columns.iter().enumerate() {
            let prop = format!(":{}", series.name());
            let lit  = anyvalue_to_turtle(&series.get(row_idx).unwrap_or(AnyValue::Null));
            let is_last = col_idx == ncols - 1;
            let terminator = if is_last { " ." } else { " ;" };

            if col_idx == 0 {
                out.push_str(&format!(":{}{} {} {}{}\n",
                    row_stem, row_idx, prop, lit, terminator));
            } else {
                out.push_str(&format!("    {} {}{}\n",
                    prop, lit, terminator));
            }
        }
    }
    out
}

/// Information about a single column.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub dtype: String,
}

/// Summary information about a DataFrame.
#[derive(Debug, Clone)]
pub struct DataFrameInfo {
    pub nrows: usize,
    pub ncols: usize,
    pub columns: Vec<ColumnInfo>,
}

/// Extract shape and column metadata from a DataFrame.
pub fn dataframe_info(df: &DataFrame) -> DataFrameInfo {
    let (nrows, ncols) = df.shape();
    let columns = df
        .columns()
        .iter()
        .map(|col| ColumnInfo {
            name: col.name().to_string(),
            dtype: format!("{}", col.dtype()),
        })
        .collect();

    DataFrameInfo { nrows, ncols, columns }
}

/// Format a `DataFrameInfo` into a human-readable string.
pub fn format_dataframe_info(info: &DataFrameInfo) -> String {
    let mut out = format!(
        "DataFrame: {} rows × {} columns\n",
        info.nrows, info.ncols
    );
    for col in &info.columns {
        out.push_str(&format!("  - {} ({})\n", col.name, col.dtype));
    }
    out
}

/// Print DataFrame information to stdout.
pub fn print_dataframe_info(df: &DataFrame) {
    print!("{}", format_dataframe_info(&dataframe_info(df)));
}

// ── Python bindings ─────────────────────────────────────────────────────────

#[cfg(feature = "python")]
mod python {
    use super::*;
    use pyo3::prelude::*;
    use pyo3_polars::PyDataFrame;

    /// Return a string describing the given Polars DataFrame.
    #[pyfunction]
    fn describe(py_df: PyDataFrame) -> PyResult<String> {
        let df: DataFrame = py_df.into();
        let info = dataframe_info(&df);
        Ok(format_dataframe_info(&info))
    }

    /// Print information about the given Polars DataFrame to stdout.
    #[pyfunction]
    fn print_info(py_df: PyDataFrame) -> PyResult<()> {
        let df: DataFrame = py_df.into();
        print_dataframe_info(&df);
        Ok(())
    }

    /// Convert a Polars DataFrame to a Turtle (RDF) string.
    ///
    /// Args:
    ///     df: Polars DataFrame to convert.
    ///     base_url: Base IRI used as the prefix (e.g. "http://example.org/").
    ///     row_stem: Local name stem for row subjects (e.g. "r" → :r0, :r1, …).
    #[pyfunction]
    fn to_turtle(py_df: PyDataFrame, base_url: &str, row_stem: &str) -> PyResult<String> {
        let df: DataFrame = py_df.into();
        Ok(dataframe_to_turtle(&df, base_url, row_stem))
    }

    #[pymodule]
    pub fn rumo(m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add_function(wrap_pyfunction!(describe, m)?)?;
        m.add_function(wrap_pyfunction!(print_info, m)?)?;
        m.add_function(wrap_pyfunction!(to_turtle, m)?)?;
        Ok(())
    }
}

#[cfg(feature = "python")]
pub use python::rumo;

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use polars::prelude::*;
    use super::*;

    #[test]
    fn test_dataframe_info_shape() {
        let df = df![
            "name" => ["Alice", "Bob", "Carol"],
            "age"  => [25i32, 30, 35]
        ]
        .unwrap();

        let info = dataframe_info(&df);
        assert_eq!(info.nrows, 3);
        assert_eq!(info.ncols, 2);
    }

    #[test]
    fn test_dataframe_info_column_names() {
        let df = df![
            "name"  => ["Alice", "Bob"],
            "score" => [1.0f64, 2.0]
        ]
        .unwrap();

        let info = dataframe_info(&df);
        assert_eq!(info.columns[0].name, "name");
        assert_eq!(info.columns[1].name, "score");
    }

    #[test]
    fn test_dataframe_info_dtypes() {
        let df = df!["x" => [1i32, 2, 3]].unwrap();

        let info = dataframe_info(&df);
        // polars 0.53 Display formats Int32 as "i32"
        assert!(
            info.columns[0].dtype.contains("i32"),
            "expected dtype to contain 'i32', got '{}'",
            info.columns[0].dtype
        );
    }

    #[test]
    fn test_format_dataframe_info_contains_shape() {
        let df = df![
            "a" => [1i32, 2],
            "b" => ["x", "y"]
        ]
        .unwrap();

        let info = dataframe_info(&df);
        let output = format_dataframe_info(&info);
        assert!(output.contains("2 rows"), "missing '2 rows' in:\n{output}");
        assert!(output.contains("2 columns"), "missing '2 columns' in:\n{output}");
    }

    #[test]
    fn test_format_dataframe_info_contains_column_names() {
        let df = df![
            "alpha" => [1i32],
            "beta"  => [2.0f64]
        ]
        .unwrap();

        let info = dataframe_info(&df);
        let output = format_dataframe_info(&info);
        assert!(output.contains("alpha"), "missing 'alpha' in:\n{output}");
        assert!(output.contains("beta"), "missing 'beta' in:\n{output}");
    }

    #[test]
    fn test_empty_dataframe() {
        let df = DataFrame::empty();
        let info = dataframe_info(&df);
        assert_eq!(info.nrows, 0);
        assert_eq!(info.ncols, 0);
        assert!(info.columns.is_empty());
    }

    // ── Turtle conversion tests ───────────────────────────────────────────────

    #[test]
    fn test_turtle_prefix_line() {
        let df = df!["name" => ["Alice"]].unwrap();
        let ttl = dataframe_to_turtle(&df, "http://example.org/", "r");
        assert!(ttl.contains("prefix : <http://example.org/>"), "missing prefix in:\n{ttl}");
    }

    #[test]
    fn test_turtle_row_subject() {
        let df = df!["name" => ["Alice"]].unwrap();
        let ttl = dataframe_to_turtle(&df, "http://example.org/", "r");
        assert!(ttl.contains(":r0"), "missing :r0 in:\n{ttl}");
    }

    #[test]
    fn test_turtle_string_value_quoted() {
        let df = df!["name" => ["Alice"]].unwrap();
        let ttl = dataframe_to_turtle(&df, "http://example.org/", "r");
        assert!(ttl.contains(":name \"Alice\""), "missing quoted string in:\n{ttl}");
    }

    #[test]
    fn test_turtle_int_value_unquoted() {
        let df = df!["age" => [25i32]].unwrap();
        let ttl = dataframe_to_turtle(&df, "http://example.org/", "r");
        assert!(ttl.contains(":age 25"), "missing int in:\n{ttl}");
        assert!(!ttl.contains(":age \"25\""), "int should not be quoted in:\n{ttl}");
    }

    #[test]
    fn test_turtle_float_value_unquoted() {
        let df = df!["score" => [88.5f64]].unwrap();
        let ttl = dataframe_to_turtle(&df, "http://example.org/", "r");
        assert!(ttl.contains(":score 88.5"), "missing float in:\n{ttl}");
        assert!(!ttl.contains(":score \"88.5\""), "float should not be quoted in:\n{ttl}");
    }

    #[test]
    fn test_turtle_float_whole_number_has_decimal() {
        let df = df!["score" => [92.0f64]].unwrap();
        let ttl = dataframe_to_turtle(&df, "http://example.org/", "r");
        assert!(ttl.contains(":score 92.0"), "float whole number should have decimal point in:\n{ttl}");
    }

    #[test]
    fn test_turtle_multiple_rows() {
        let df = df!["name" => ["Alice", "Bob"]].unwrap();
        let ttl = dataframe_to_turtle(&df, "http://example.org/", "r");
        assert!(ttl.contains(":r0"), "missing :r0 in:\n{ttl}");
        assert!(ttl.contains(":r1"), "missing :r1 in:\n{ttl}");
    }

    #[test]
    fn test_turtle_multiple_columns_semicolons() {
        let df = df![
            "name" => ["Alice"],
            "age"  => [25i32],
        ].unwrap();
        let ttl = dataframe_to_turtle(&df, "http://example.org/", "r");
        // first property on subject line, separated by semicolon
        assert!(ttl.contains(":r0 :name \"Alice\" ;"), "missing first property with semicolon in:\n{ttl}");
        // last property ends with dot
        assert!(ttl.contains(":age 25 ."), "missing last property with dot in:\n{ttl}");
    }

    #[test]
    fn test_turtle_sample_full() {
        let df = df![
            "name"  => ["Alice", "Bob", "Carol", "Dave", "Eve"],
            "age"   => [25i32, 30, 22, 35, 28],
            "score" => [88.5f64, 92.0, 76.3, 95.1, 81.7],
        ].unwrap();
        let ttl = dataframe_to_turtle(&df, "http://example.org/", "r");
        assert!(ttl.contains(":r0 :name \"Alice\" ;"), "r0 name in:\n{ttl}");
        assert!(ttl.contains("    :age 25 ;"), "r0 age in:\n{ttl}");
        assert!(ttl.contains("    :score 88.5 ."), "r0 score in:\n{ttl}");
        assert!(ttl.contains(":r4 :name \"Eve\" ;"), "r4 name in:\n{ttl}");
        assert!(ttl.contains("    :score 81.7 ."), "r4 score in:\n{ttl}");
    }
}
