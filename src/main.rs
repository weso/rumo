use clap::{Parser, Subcommand};
use polars::prelude::{CsvReadOptions, SerReader};
use rumo::{dataframe_info, dataframe_to_turtle, format_dataframe_info};
use std::io::Write;

#[derive(Parser)]
#[command(name = "rumo", about = "Read and transform DataFrames")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Describe the structure of a data file
    Describe {
        /// Path to the input data file
        #[arg(long)]
        data: String,

        /// Input format (currently only "csv" is supported)
        #[arg(long, default_value = "csv")]
        format: String,
    },

    /// Execute a Nemo rule file (.rls)
    Rules {
        /// Path to the rule file
        #[arg(long)]
        rules: String,

        /// Path to the input data file (its directory is used to resolve @import paths)
        #[arg(long)]
        data: Option<String>,

        /// Path to write the output (prints to stdout if omitted)
        #[arg(long)]
        output: Option<String>,

        /// Global parameter assignment in the form KEY=VALUE (may be repeated)
        #[arg(long = "param", value_name = "KEY=VALUE")]
        params: Vec<String>,
    },

    /// Convert a data file to another format
    Convert {
        /// Path to the input data file
        #[arg(long)]
        data: String,

        /// Input format (currently only "csv" is supported)
        #[arg(long, default_value = "csv")]
        format: String,

        /// Output format (currently only "turtle" is supported)
        #[arg(long)]
        result_format: String,

        /// Path to write the output (prints to stdout if omitted)
        #[arg(long)]
        output: Option<String>,

        /// Base IRI used as the prefix in Turtle output
        #[arg(long, default_value = "http://example.org/")]
        base_url: String,

        /// Local name stem for row subjects (e.g. "r" produces :r0, :r1, …)
        #[arg(long, default_value = "r")]
        stem: String,
    },
}

fn read_csv(path: &str) -> polars::prelude::DataFrame {
    CsvReadOptions::default()
        .try_into_reader_with_file_path(Some(path.into()))
        .and_then(|r| r.finish())
        .unwrap_or_else(|e| {
            eprintln!("Error reading CSV '{path}': {e}");
            std::process::exit(1);
        })
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Rules { rules, data, output, params } => {
            #[cfg(feature = "rules")]
            {
                let parsed_params: Vec<(String, String)> = params
                    .iter()
                    .map(|s| {
                        let (k, v) = s.split_once('=').unwrap_or_else(|| {
                            eprintln!("Invalid --param value '{s}': expected KEY=VALUE");
                            std::process::exit(1);
                        });
                        (k.to_string(), v.to_string())
                    })
                    .collect();
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    let data_path = data.as_deref().map(std::path::Path::new);
                    let output_path = output.as_deref().map(std::path::Path::new);
                    if let Err(e) = rumo::rules::run_rules_file(
                        std::path::Path::new(&rules),
                        data_path,
                        output_path,
                        parsed_params,
                    )
                    .await
                    {
                        eprintln!("Error running rules: {e}");
                        std::process::exit(1);
                    }
                });
            }
            #[cfg(not(feature = "rules"))]
            {
                let _ = (rules, data, output, params);
                eprintln!("Rule support is not enabled. Rebuild with --features rules.");
                std::process::exit(1);
            }
        }

        Command::Describe { data, format } => {
            if format != "csv" {
                eprintln!("Unsupported input format: {format}");
                std::process::exit(1);
            }
            let df = read_csv(&data);
            print!("{}", format_dataframe_info(&dataframe_info(&df)));
        }

        Command::Convert { data, format, result_format, output, base_url, stem } => {
            if format != "csv" {
                eprintln!("Unsupported input format: {format}");
                std::process::exit(1);
            }
            if result_format != "turtle" {
                eprintln!("Unsupported result format: {result_format}");
                std::process::exit(1);
            }
            let df = read_csv(&data);
            let turtle = dataframe_to_turtle(&df, &base_url, &stem);
            match output {
                Some(path) => {
                    std::fs::write(&path, &turtle).unwrap_or_else(|e| {
                        eprintln!("Error writing to '{path}': {e}");
                        std::process::exit(1);
                    });
                }
                None => {
                    std::io::stdout().write_all(turtle.as_bytes()).unwrap();
                }
            }
        }
    }
}
