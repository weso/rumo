use std::path::{Path, PathBuf};
use std::sync::Arc;

use nemo::datavalues::{AnyDataValue, DataValue, ValueDomain};
use polars::prelude::DataFrame;
use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};
use pyo3::IntoPyObjectExt;
use pyo3_polars::PyDataFrame;

use crate::rules::{run_rules, DataResources, RuleTable};
use crate::{dataframe_info, dataframe_to_turtle, format_dataframe_info, print_dataframe_info};

const DEFAULT_BASE_URL: &str = "http://example.org/";
const DEFAULT_ROW_STEM: &str = "r";
const DEFAULT_RESOURCE: &str = "data.ttl";
const XSD_DECIMAL: &str = "http://www.w3.org/2001/XMLSchema#decimal";

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

// ── Rumo ─────────────────────────────────────────────────────────────────────

/// Accept a Polars DataFrame, or a pandas DataFrame (converted through Polars).
fn extract_dataframe(df: &Bound<'_, PyAny>) -> PyResult<DataFrame> {
    if let Ok(py_df) = df.extract::<PyDataFrame>() {
        return Ok(py_df.into());
    }
    if !df.hasattr("to_dict")? || !df.hasattr("columns")? {
        return Err(PyValueError::new_err(
            "expected a pandas or polars DataFrame",
        ));
    }
    let py = df.py();
    let polars = py.import("polars")?;
    let converted = match polars.call_method1("from_pandas", (df,)) {
        Ok(converted) => converted,
        // `polars.from_pandas` needs pyarrow for non-numpy columns; go through Python lists instead.
        Err(err) if err.is_instance_of::<pyo3::exceptions::PyImportError>(py) => {
            let columns = PyDict::new(py);
            for column in df.getattr("columns")?.try_iter()? {
                let column = column?;
                let values = df.get_item(&column)?.call_method0("tolist")?;
                columns.set_item(column.str()?, values)?;
            }
            polars.call_method1("DataFrame", (columns,))?
        }
        Err(err) => return Err(err),
    };
    Ok(converted.extract::<PyDataFrame>()?.into())
}

/// Convert a Python parameter value to nemo term syntax.
///
/// Strings become string literals, except `<...>` which is passed through as an IRI.
fn param_to_term(key: &str, value: &Bound<'_, PyAny>) -> PyResult<String> {
    if value.is_instance_of::<PyBool>() {
        let b: bool = value.extract()?;
        return Ok(format!("\"{b}\"^^<http://www.w3.org/2001/XMLSchema#boolean>"));
    }
    if value.is_instance_of::<PyInt>() || value.is_instance_of::<PyFloat>() {
        return Ok(value.str()?.to_string());
    }
    if value.is_instance_of::<PyString>() {
        let s: String = value.extract()?;
        if s.starts_with('<') && s.ends_with('>') {
            return Ok(s);
        }
        return Ok(quote(&s));
    }
    Err(PyValueError::new_err(format!(
        "parameter '{key}' must be a str, int, float or bool"
    )))
}

/// Quote a string as a nemo/Turtle string literal.
fn quote(s: &str) -> String {
    let escaped = s
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r");
    format!("\"{escaped}\"")
}

fn value_to_python(py: Python<'_>, value: &AnyDataValue) -> PyResult<Py<PyAny>> {
    match value.value_domain() {
        ValueDomain::PlainString => value.to_plain_string_unchecked().into_py_any(py),
        ValueDomain::Iri => value.to_iri_unchecked().into_py_any(py),
        ValueDomain::LanguageTaggedString => {
            let (text, tag) = value.to_language_tagged_string_unchecked();
            format!("{}@{tag}", quote(&text)).into_py_any(py)
        }
        ValueDomain::Double => value.to_f64_unchecked().into_py_any(py),
        ValueDomain::Float => value.to_f32_unchecked().into_py_any(py),
        ValueDomain::UnsignedLong => value.to_u64_unchecked().into_py_any(py),
        ValueDomain::NonNegativeLong
        | ValueDomain::UnsignedInt
        | ValueDomain::NonNegativeInt
        | ValueDomain::Long
        | ValueDomain::Int => value.to_i64_unchecked().into_py_any(py),
        ValueDomain::Boolean => value.to_boolean_unchecked().into_py_any(py),
        // Turtle reads numbers like `88.5` as xsd:decimal.
        ValueDomain::Other if value.datatype_iri() == XSD_DECIMAL => {
            match value.lexical_value().parse::<f64>() {
                Ok(number) => number.into_py_any(py),
                Err(_) => value.to_string().into_py_any(py),
            }
        }
        ValueDomain::Null | ValueDomain::Tuple | ValueDomain::Map | ValueDomain::Other => {
            value.to_string().into_py_any(py)
        }
    }
}

/// Data registered with a [Rumo] object.
#[derive(Clone)]
struct DataEntry {
    resource: String,
    format: String,
    content: Arc<[u8]>,
    /// If set, the data is imported into this predicate without an `@import` in the rules.
    predicate: Option<String>,
}

#[derive(Clone)]
struct Rules {
    text: String,
    /// File the rules came from; relative `@import` paths resolve against its directory.
    path: Option<PathBuf>,
}

/// Holds a nemo rule program and the data it runs on.
///
/// Rules can be given as a string (`rules`) or a file (`rules_file`).
/// Data can be a pandas or polars DataFrame (converted to Turtle), RDF text,
/// or a file (`data_file`).
///
/// Data is made available to the rules as an in-memory resource named
/// `resource` (by default `data.ttl`, or the file name for `data_file`), so the
/// rules can load it with `@import p :- turtle { resource = "data.ttl" } .`.
/// Alternatively pass `predicate="p"` and Rumo adds that import itself.
///
/// Example:
///     >>> rumo = Rumo(rules=rules, data=df, predicate="data")
///     >>> rumo.run(params={"GOOD_SCORE": 90}).to_pandas("good")
#[pyclass(module = "pyrumo")]
struct Rumo {
    rules: Option<Rules>,
    data: Vec<DataEntry>,
}

impl Rumo {
    fn add_data(&mut self, entry: DataEntry) {
        self.data.retain(|e| e.resource != entry.resource);
        self.data.push(entry);
    }

    /// The rule program, followed by the `@import` statements for data loaded with a predicate.
    fn program(&self) -> PyResult<String> {
        let rules = self
            .rules
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("no rules loaded"))?;
        let mut program = rules.text.clone();
        for entry in &self.data {
            if let Some(predicate) = &entry.predicate {
                program.push_str(&format!(
                    "\n@import {predicate} :- {} {{ resource = {} }} .\n",
                    entry.format,
                    quote(&entry.resource)
                ));
            }
        }
        Ok(program)
    }
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.to_string_lossy().into_owned())
}

#[pymethods]
impl Rumo {
    /// Create a Rumo object, optionally loading rules and data.
    ///
    /// Args:
    ///     rules: Rule program as a string.
    ///     rules_file: Path to a rule file (.rls).
    ///     data: A pandas/polars DataFrame, or RDF data as a string.
    ///     data_file: Path to a data file.
    ///     format: nemo import format of `data`/`data_file` (e.g. "turtle", "ntriples", "csv").
    ///     resource: Resource name the rules use to import the data.
    ///     predicate: If given, import the data into this predicate automatically.
    ///     base_url: Base IRI used when converting a DataFrame to RDF.
    ///     row_stem: Local name stem for DataFrame row subjects.
    #[new]
    #[pyo3(signature = (rules=None, rules_file=None, data=None, data_file=None, *,
        format=None, resource=None, predicate=None,
        base_url=DEFAULT_BASE_URL, row_stem=DEFAULT_ROW_STEM))]
    #[allow(clippy::too_many_arguments)]
    fn py_new(
        rules: Option<String>,
        rules_file: Option<PathBuf>,
        data: Option<&Bound<'_, PyAny>>,
        data_file: Option<PathBuf>,
        format: Option<String>,
        resource: Option<String>,
        predicate: Option<String>,
        base_url: &str,
        row_stem: &str,
    ) -> PyResult<Self> {
        if rules.is_some() && rules_file.is_some() {
            return Err(PyValueError::new_err("pass either rules or rules_file, not both"));
        }
        if data.is_some() && data_file.is_some() {
            return Err(PyValueError::new_err("pass either data or data_file, not both"));
        }
        let mut rumo = Rumo { rules: None, data: Vec::new() };
        if let Some(rules) = rules {
            rumo.read_rules_str(rules);
        }
        if let Some(path) = rules_file {
            rumo.read_rules_file(path)?;
        }
        if let Some(path) = data_file {
            rumo.read_data_file(path, format, resource, predicate)?;
        } else if let Some(data) = data {
            if data.is_instance_of::<PyString>() {
                let format = format.unwrap_or_else(|| "turtle".to_string());
                rumo.read_data_str(&data.extract::<String>()?, &format, resource, predicate);
            } else {
                rumo.read_dataframe(data, resource, predicate, base_url, row_stem)?;
            }
        }
        Ok(rumo)
    }

    /// Load the rule program from a string, replacing any rules loaded before.
    fn read_rules_str(&mut self, rules: String) {
        self.rules = Some(Rules { text: rules, path: None });
    }

    /// Load the rule program from a file, replacing any rules loaded before.
    fn read_rules_file(&mut self, path: PathBuf) -> PyResult<()> {
        let text = std::fs::read_to_string(&path)?;
        self.rules = Some(Rules { text, path: Some(path) });
        Ok(())
    }

    /// Add a pandas or polars DataFrame, converted to Turtle.
    ///
    /// Each row becomes a subject `<base_url><row_stem><index>` with one
    /// property `<base_url><column>` per column.
    #[pyo3(signature = (df, resource=None, predicate=None,
        base_url=DEFAULT_BASE_URL, row_stem=DEFAULT_ROW_STEM))]
    fn read_dataframe(
        &mut self,
        df: &Bound<'_, PyAny>,
        resource: Option<String>,
        predicate: Option<String>,
        base_url: &str,
        row_stem: &str,
    ) -> PyResult<()> {
        let df = extract_dataframe(df)?;
        let turtle = dataframe_to_turtle(&df, base_url, row_stem);
        self.add_data(DataEntry {
            resource: resource.unwrap_or_else(|| DEFAULT_RESOURCE.to_string()),
            format: "turtle".to_string(),
            content: turtle.into_bytes().into(),
            predicate,
        });
        Ok(())
    }

    /// Add data given as a string (Turtle by default).
    #[pyo3(signature = (data, format="turtle", resource=None, predicate=None))]
    fn read_data_str(
        &mut self,
        data: &str,
        format: &str,
        resource: Option<String>,
        predicate: Option<String>,
    ) {
        self.add_data(DataEntry {
            resource: resource.unwrap_or_else(|| DEFAULT_RESOURCE.to_string()),
            format: format.to_string(),
            content: data.as_bytes().into(),
            predicate,
        });
    }

    /// Add data from a file. Its resource name defaults to the file name.
    #[pyo3(signature = (path, format=None, resource=None, predicate=None))]
    fn read_data_file(
        &mut self,
        path: PathBuf,
        format: Option<String>,
        resource: Option<String>,
        predicate: Option<String>,
    ) -> PyResult<()> {
        let content = std::fs::read(&path)?;
        let format = format.unwrap_or_else(|| match path.extension().and_then(|e| e.to_str()) {
            Some("nt") => "ntriples".to_string(),
            Some("nq") => "nquads".to_string(),
            Some("rdf") | Some("xml") => "rdfxml".to_string(),
            Some(ext @ ("csv" | "tsv" | "trig" | "json")) => ext.to_string(),
            _ => "turtle".to_string(),
        });
        self.add_data(DataEntry {
            resource: resource.unwrap_or_else(|| file_name(&path)),
            format,
            content: content.into(),
            predicate,
        });
        Ok(())
    }

    /// Remove the rule program.
    fn reset_rules(&mut self) {
        self.rules = None;
    }

    /// Remove all data.
    fn reset_data(&mut self) {
        self.data.clear();
    }

    /// Resource names of the loaded data.
    #[getter]
    fn resources(&self) -> Vec<String> {
        self.data.iter().map(|e| e.resource.clone()).collect()
    }

    /// Run the rules over the data.
    ///
    /// Args:
    ///     params: Values for the `$NAME` global parameters of the rules.
    ///         Strings are passed as string literals, except `"<...>"` which is an IRI.
    ///     predicates: Predicates to return. Defaults to the `@output`/`@export`
    ///         predicates of the rules, or all derived predicates if there are none.
    ///
    /// Returns:
    ///     RumoResults with the facts of each predicate.
    #[pyo3(signature = (params=None, predicates=None))]
    fn run(
        &self,
        py: Python<'_>,
        params: Option<&Bound<'_, PyDict>>,
        predicates: Option<Vec<String>>,
    ) -> PyResult<RumoResults> {
        let program = self.program()?;
        let rules = self.rules.as_ref().expect("checked by program()");
        let (rules_name, base_path) = match &rules.path {
            Some(path) => (
                path.to_string_lossy().into_owned(),
                path.parent().map(Path::to_path_buf),
            ),
            None => (String::new(), None),
        };

        let mut global_params = Vec::new();
        for (key, value) in params.into_iter().flat_map(|p| p.iter()) {
            let key: String = key.extract()?;
            let term = param_to_term(&key, &value)?;
            global_params.push((key, term));
        }

        let resources: DataResources = self
            .data
            .iter()
            .map(|e| (e.resource.clone(), e.content.clone()))
            .collect();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let tables = rt
            .block_on(run_rules(
                &program,
                &rules_name,
                base_path,
                resources,
                global_params,
                predicates,
            ))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        RumoResults::from_tables(py, tables)
    }

    fn __repr__(&self) -> String {
        let rules = match &self.rules {
            Some(Rules { path: Some(path), .. }) => format!("'{}'", path.display()),
            Some(_) => "<string>".to_string(),
            None => "None".to_string(),
        };
        format!("Rumo(rules={rules}, resources={:?})", self.resources())
    }
}

struct ResultTable {
    predicate: String,
    columns: Vec<String>,
    rows: Vec<Py<PyTuple>>,
}

/// Facts derived by `Rumo.run`, grouped by predicate.
///
/// Behaves like a read-only mapping from predicate name to a list of
/// dictionaries (one per fact, keyed by column name). Column names come from
/// the variables in the rule heads, e.g. `good(?name, ?score)` gives the
/// columns `name` and `score`.
#[pyclass(module = "pyrumo")]
struct RumoResults {
    tables: Vec<ResultTable>,
}

impl RumoResults {
    fn from_tables(py: Python<'_>, tables: Vec<RuleTable>) -> PyResult<Self> {
        let tables = tables
            .into_iter()
            .map(|table| {
                let rows = table
                    .rows
                    .iter()
                    .map(|row| {
                        let values = row
                            .iter()
                            .map(|v| value_to_python(py, v))
                            .collect::<PyResult<Vec<_>>>()?;
                        Ok(PyTuple::new(py, values)?.unbind())
                    })
                    .collect::<PyResult<Vec<_>>>()?;
                Ok(ResultTable {
                    predicate: table.predicate,
                    columns: table.columns,
                    rows,
                })
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(RumoResults { tables })
    }

    /// Look up a table; `None` is allowed when there is exactly one.
    fn table(&self, predicate: Option<&str>) -> PyResult<&ResultTable> {
        match predicate {
            Some(name) => self
                .tables
                .iter()
                .find(|t| t.predicate == name)
                .ok_or_else(|| PyKeyError::new_err(name.to_string())),
            None if self.tables.len() == 1 => Ok(&self.tables[0]),
            None => Err(PyValueError::new_err(format!(
                "results contain several predicates, choose one of {:?}",
                self.predicates()
            ))),
        }
    }

    /// Column names, checking the length of any user-given override.
    fn column_names(table: &ResultTable, columns: Option<Vec<String>>) -> PyResult<Vec<String>> {
        match columns {
            Some(columns) if columns.len() != table.columns.len() => {
                Err(PyValueError::new_err(format!(
                    "predicate '{}' has {} columns, got {} names",
                    table.predicate,
                    table.columns.len(),
                    columns.len()
                )))
            }
            Some(columns) => Ok(columns),
            None => Ok(table.columns.clone()),
        }
    }

    fn dicts<'py>(
        py: Python<'py>,
        table: &ResultTable,
        columns: &[String],
    ) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty(py);
        for row in &table.rows {
            let dict = PyDict::new(py);
            for (column, value) in columns.iter().zip(row.bind(py).iter()) {
                dict.set_item(column, value)?;
            }
            list.append(dict)?;
        }
        Ok(list)
    }
}

#[pymethods]
impl RumoResults {
    /// Names of the predicates in the results.
    fn predicates(&self) -> Vec<String> {
        self.tables.iter().map(|t| t.predicate.clone()).collect()
    }

    /// Column names of a predicate.
    #[pyo3(signature = (predicate=None))]
    fn columns(&self, predicate: Option<&str>) -> PyResult<Vec<String>> {
        Ok(self.table(predicate)?.columns.clone())
    }

    /// Facts of a predicate as a list of tuples.
    #[pyo3(signature = (predicate=None))]
    fn rows(&self, py: Python<'_>, predicate: Option<&str>) -> PyResult<Vec<Py<PyTuple>>> {
        Ok(self
            .table(predicate)?
            .rows
            .iter()
            .map(|r| r.clone_ref(py))
            .collect())
    }

    /// Facts of a predicate as a list of dictionaries.
    ///
    /// `predicate` may be omitted when the results contain a single predicate.
    /// `columns` overrides the column names.
    #[pyo3(signature = (predicate=None, columns=None))]
    fn to_dicts<'py>(
        &self,
        py: Python<'py>,
        predicate: Option<&str>,
        columns: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let table = self.table(predicate)?;
        let columns = Self::column_names(table, columns)?;
        Self::dicts(py, table, &columns)
    }

    /// Facts of a predicate as a pandas DataFrame (requires pandas).
    #[pyo3(signature = (predicate=None, columns=None))]
    fn to_pandas<'py>(
        &self,
        py: Python<'py>,
        predicate: Option<&str>,
        columns: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let table = self.table(predicate)?;
        let columns = Self::column_names(table, columns)?;
        let rows = PyList::new(py, table.rows.iter().map(|r| r.bind(py)))?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("columns", PyList::new(py, &columns)?)?;
        py.import("pandas")?
            .getattr("DataFrame")?
            .call((rows,), Some(&kwargs))
    }

    /// Facts of a predicate as a polars DataFrame.
    #[pyo3(signature = (predicate=None, columns=None))]
    fn to_polars<'py>(
        &self,
        py: Python<'py>,
        predicate: Option<&str>,
        columns: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let table = self.table(predicate)?;
        let columns = Self::column_names(table, columns)?;
        let rows = PyList::new(py, table.rows.iter().map(|r| r.bind(py)))?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("schema", PyList::new(py, &columns)?)?;
        kwargs.set_item("orient", "row")?;
        py.import("polars")?
            .getattr("DataFrame")?
            .call((rows,), Some(&kwargs))
    }

    /// All results as `{predicate: list of dictionaries}`.
    fn to_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for table in &self.tables {
            dict.set_item(&table.predicate, Self::dicts(py, table, &table.columns)?)?;
        }
        Ok(dict)
    }

    fn __getitem__<'py>(&self, py: Python<'py>, predicate: &str) -> PyResult<Bound<'py, PyList>> {
        let table = self.table(Some(predicate))?;
        Self::dicts(py, table, &table.columns)
    }

    fn __contains__(&self, predicate: &str) -> bool {
        self.tables.iter().any(|t| t.predicate == predicate)
    }

    fn __len__(&self) -> usize {
        self.tables.len()
    }

    fn __iter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        PyList::new(py, self.predicates())?.into_any().try_iter().map(Bound::into_any)
    }

    fn __repr__(&self) -> String {
        let tables: Vec<String> = self
            .tables
            .iter()
            .map(|t| format!("{}({}): {} rows", t.predicate, t.columns.join(", "), t.rows.len()))
            .collect();
        format!("RumoResults({})", tables.join("; "))
    }
}

#[pymodule]
pub fn pyrumo(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(describe, m)?)?;
    m.add_function(wrap_pyfunction!(print_info, m)?)?;
    m.add_function(wrap_pyfunction!(to_turtle, m)?)?;
    m.add_class::<Rumo>()?;
    m.add_class::<RumoResults>()?;
    Ok(())
}
