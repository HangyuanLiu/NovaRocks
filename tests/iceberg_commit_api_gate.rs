use std::fs;
use std::path::{Path, PathBuf};

fn collect_rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("read source directory") {
        let entry = entry.expect("read source entry");
        let path = entry.path();
        if path.is_dir() {
            collect_rust_files(&path, out);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

#[test]
fn production_sources_use_canonical_run_iceberg_commit_name() {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    collect_rust_files(&repo.join("src"), &mut files);

    let mut offenders = Vec::new();
    for file in files {
        let text = fs::read_to_string(&file).expect("read rust source");
        for (idx, line) in text.lines().enumerate() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("//") || trimmed.starts_with("*") {
                continue;
            }
            if line.contains("run_iceberg_commit_typed") {
                let rel = file.strip_prefix(&repo).expect("relative source path");
                offenders.push(format!("{}:{}", rel.display(), idx + 1));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "production code should use canonical run_iceberg_commit, not migration-only run_iceberg_commit_typed:\n{}",
        offenders.join("\n")
    );
}
