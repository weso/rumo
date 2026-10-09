use std::collections::HashMap;
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use nemo::{
    datavalues::AnyDataValue,
    error::ReadingError,
    execution::{DefaultExecutionEngine, execution_parameters::ExecutionParameters},
    io::{
        ExportManager, ImportManager,
        resource_providers::{ResourceProvider, ResourceProviders, file, http, stdin},
    },
    rule_file::RuleFile,
    rule_model::{
        components::{
            tag::Tag,
            term::{Term, primitive::Primitive},
        },
        programs::{ProgramRead, program::Program},
    },
};
use nemo_physical::resource::Resource;

/// In-memory contents that rule programs can `@import` by resource name.
pub type DataResources = HashMap<String, Arc<[u8]>>;

/// Serves [DataResources] to nemo before falling back to files, HTTP and stdin.
#[derive(Debug)]
struct InMemoryResourceProvider(DataResources);

#[async_trait::async_trait(?Send)]
impl ResourceProvider for InMemoryResourceProvider {
    async fn open_resource(
        &self,
        resource: &Resource,
        _media_type: &str,
    ) -> Result<Option<Box<dyn Read>>, ReadingError> {
        let Resource::Path(path) = resource else {
            return Ok(None);
        };
        Ok(self
            .0
            .get(path.to_string_lossy().as_ref())
            .map(|content| Box::new(Cursor::new(content.clone())) as Box<dyn Read>))
    }
}

/// The facts derived for one predicate.
#[derive(Debug, Clone)]
pub struct RuleTable {
    pub predicate: String,
    /// Column names, taken from the variable names used in rule heads
    /// (`col0`, `col1`, … where no name is available).
    pub columns: Vec<String>,
    pub rows: Vec<Vec<AnyDataValue>>,
}

/// Name the columns of `predicate` after the variables used in the rule heads that derive it.
fn column_names(program: &Program, predicate: &Tag, arity: usize) -> Vec<String> {
    let mut names: Vec<Option<String>> = vec![None; arity];
    let heads = program
        .rules()
        .flat_map(|rule| rule.head())
        .filter(|atom| atom.predicate() == *predicate);
    for atom in heads {
        for (index, term) in atom.terms().enumerate().take(arity) {
            if names[index].is_some() {
                continue;
            }
            if let Term::Primitive(Primitive::Variable(variable)) = term {
                if let Some(name) = variable.name() {
                    if !names.iter().flatten().any(|n| n == name) {
                        names[index] = Some(name.to_string());
                    }
                }
            }
        }
    }
    names
        .into_iter()
        .enumerate()
        .map(|(index, name)| name.unwrap_or_else(|| format!("col{index}")))
        .collect()
}

/// Render program errors with their source locations instead of just the first message.
fn describe_error(error: nemo::error::Error) -> Box<dyn std::error::Error> {
    if let nemo::error::Error::ProgramReport(report) = &error {
        let mut out = Vec::new();
        if report.write(&mut out).is_ok() && !out.is_empty() {
            return String::from_utf8_lossy(&out).trim_end().to_string().into();
        }
    }
    error.into()
}

/// Run a nemo rule program and collect the derived facts.
///
/// * `rules_name` – label used in error messages (e.g. the rule file path).
/// * `base_path` – directory against which relative `@import` paths resolve.
/// * `resources` – in-memory data, imported by resource name before any file lookup.
/// * `predicates` – predicates to collect. If `None`, the `@output` and `@export`
///   predicates of the program are collected, or every derived predicate if it has none.
pub async fn run_rules(
    rules: &str,
    rules_name: &str,
    base_path: Option<PathBuf>,
    resources: DataResources,
    global_params: Vec<(String, String)>,
    predicates: Option<Vec<String>>,
) -> Result<Vec<RuleTable>, Box<dyn std::error::Error>> {
    let providers = ResourceProviders::from(vec![
        Box::new(InMemoryResourceProvider(resources)),
        Box::<http::HttpResourceProvider>::default(),
        Box::new(file::FileResourceProvider::new(base_path)),
        Box::new(stdin::StdinResourceProvider::default()),
    ]);
    let mut params = ExecutionParameters::default();
    params.set_import_manager(ImportManager::new(providers));
    if let Err(bad_key) = params.set_global(global_params.into_iter()) {
        return Err(format!("Invalid value for parameter '{bad_key}'").into());
    }

    let file = RuleFile::new(rules.to_string(), rules_name.to_string());
    let (mut engine, _warnings) = DefaultExecutionEngine::from_file(file, params)
        .await
        .map_err(describe_error)?
        .into_pair();
    engine.execute().await?;

    let program = engine.original_program_handle().materialize();
    let predicates: Vec<Tag> = match predicates {
        Some(names) => names.into_iter().map(Tag::new).collect(),
        None => {
            let mut tags: Vec<Tag> = program.outputs().map(|o| o.predicate().clone()).collect();
            for (tag, _) in engine.exports() {
                if !tags.contains(&tag) {
                    tags.push(tag);
                }
            }
            if tags.is_empty() {
                tags = program.derived_predicates().into_iter().collect();
                tags.sort_by(|a, b| a.name().cmp(b.name()));
            }
            tags
        }
    };

    let mut tables = Vec::with_capacity(predicates.len());
    for predicate in predicates {
        let Some(arity) = engine.predicate_arity(&predicate) else {
            return Err(format!("Unknown predicate '{predicate}'").into());
        };
        let rows: Vec<Vec<AnyDataValue>> = engine
            .predicate_rows(&predicate)
            .await?
            .into_iter()
            .flatten()
            .collect();
        tables.push(RuleTable {
            predicate: predicate.to_string(),
            columns: column_names(&program, &predicate, arity),
            rows,
        });
    }
    Ok(tables)
}

/// Execute a Nemo rule file (`.rls`).
///
/// `data_path`: if given, its parent directory is used as the import base so
/// that `@import` directives in the rule file resolve relative to it.
/// Otherwise imports resolve relative to the rule file's own directory.
///
/// `output_path`: if given, its parent directory is used as the export base so
/// that `@export` directives write there. If `None`, all exported files are
/// written to a temporary directory and their contents are printed to stdout.
pub async fn run_rules_file(
    rules_path: &Path,
    data_path: Option<&Path>,
    output_path: Option<&Path>,
    global_params: Vec<(String, String)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let rules_base = rules_path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::from("."));

    let import_base = data_path
        .and_then(|p| p.parent())
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| rules_base.clone());

    let program_file = RuleFile::load(rules_path.to_path_buf())?;

    let import_manager = ImportManager::new(ResourceProviders::with_base_path(Some(import_base)));
    let mut params = ExecutionParameters::default();
    params.set_import_manager(import_manager);
    if let Err(bad_key) = params.set_global(global_params.into_iter()) {
        return Err(format!("Invalid value for parameter '{bad_key}'").into());
    }

    let (mut engine, _warnings) = DefaultExecutionEngine::from_file(program_file, params)
        .await?
        .into_pair();

    engine.execute().await?;

    let export_base = match output_path {
        Some(p) => p
            .parent()
            .map(|d| d.to_path_buf())
            .unwrap_or_else(|| std::path::PathBuf::from(".")),
        None => {
            // Write to a temp dir; we'll print the results afterwards.
            let tmp = std::env::temp_dir().join(format!("rumo-{}", std::process::id()));
            std::fs::create_dir_all(&tmp)?;
            tmp
        }
    };

    let export_manager = ExportManager::default()
        .set_base_path(export_base.clone())
        .overwrite(true);

    for (predicate, handler) in engine.exports() {
        export_manager.export_table(
            &predicate,
            &handler,
            engine.predicate_rows(&predicate).await?,
        )?;
    }

    // If no output path was given, print every exported file to stdout.
    if output_path.is_none() {
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        let mut entries: Vec<_> = std::fs::read_dir(&export_base)?.collect::<Result<_, _>>()?;
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let content = std::fs::read(entry.path())?;
            out.write_all(&content)?;
        }
        let _ = std::fs::remove_dir_all(&export_base);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nemo::datavalues::DataValue;

    #[tokio::test(flavor = "current_thread")]
    async fn test_run_rules_with_in_memory_resource() {
        let rules = r#"
            @prefix : <http://example.org/> .
            @import r :- turtle { resource = "people.ttl" } .
            adult(?name) :- r(?p, :name, ?name), r(?p, :age, ?age), ?age >= $MIN .
        "#;
        let data = r#"
            @prefix : <http://example.org/> .
            :a :name "Alice" ; :age 25 .
            :b :name "Bob" ; :age 15 .
        "#;
        let resources = DataResources::from([("people.ttl".to_string(), data.as_bytes().into())]);
        let params = vec![("MIN".to_string(), "18".to_string())];

        let tables = run_rules(rules, "test", None, resources, params, None)
            .await
            .unwrap();

        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].predicate, "adult");
        assert_eq!(tables[0].columns, vec!["name"]);
        let names: Vec<String> = tables[0]
            .rows
            .iter()
            .map(|row| row[0].to_plain_string_unchecked())
            .collect();
        assert_eq!(names, vec!["Alice"]);
    }
}
