use std::io::Write;
use std::path::Path;

use nemo::{
    execution::{DefaultExecutionEngine, execution_parameters::ExecutionParameters},
    io::{ExportManager, ImportManager, resource_providers::ResourceProviders},
    rule_file::RuleFile,
};

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
