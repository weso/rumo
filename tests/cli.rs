use std::path::{Path, PathBuf};
use std::process::Command;

fn examples() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("examples")
}

fn rumo(args: &[&str]) -> String {
    let out = Command::new(env!("CARGO_BIN_EXE_rumo"))
        .args(args)
        .output()
        .expect("failed to run rumo");
    assert!(
        out.status.success(),
        "rumo {args:?} failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

#[test]
fn describe_sample_csv() {
    let csv = examples().join("sample.csv");
    let out = rumo(&["describe", "--data", csv.to_str().unwrap()]);
    assert!(out.contains("5 rows × 3 columns"), "got:\n{out}");
    assert!(out.contains("score (f64)"), "got:\n{out}");
}

#[test]
fn convert_sample_csv_matches_example_turtle() {
    let csv = examples().join("sample.csv");
    let out = rumo(&["convert", "--data", csv.to_str().unwrap(), "--result-format", "turtle"]);
    let expected = std::fs::read_to_string(examples().join("sample.ttl")).unwrap();
    assert_eq!(out.replace("\r\n", "\n"), expected.replace("\r\n", "\n"));
}

#[cfg(feature = "rules")]
#[test]
fn rules_sample_matches_expected_results() {
    let rls = examples().join("sample.rls");
    let csv = examples().join("sample.csv");
    let out = rumo(&[
        "rules",
        "--rules",
        rls.to_str().unwrap(),
        "--data",
        csv.to_str().unwrap(),
        "--param",
        "GOOD_SCORE=90",
    ]);
    let expected = std::fs::read_to_string(examples().join("results").join("good.csv")).unwrap();
    assert_eq!(out.replace("\r\n", "\n"), expected.replace("\r\n", "\n"));
}
