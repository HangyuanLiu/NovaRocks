// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

use std::{
    fs,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
};

use novarocks_parser::{TokenKind, lex};

const MUTATION_SEED: u64 = 0x5A11_C0DE_2026_0818;

#[test]
fn lex_smoke_sql_suite_corpus_is_lossless() {
    let repo_root = repo_root();
    let files = sql_suite_files(&repo_root);
    assert!(
        !files.is_empty(),
        "no SQL corpus files found under {}",
        repo_root.join("tests/sql/suites").display()
    );

    let failures = files
        .iter()
        .flat_map(|path| corpus_failures(&repo_root, path))
        .collect::<Vec<_>>();

    assert!(
        failures.is_empty(),
        "lex-smoke failures (expected empty exception list):\n{}",
        failures.join("\n")
    );
}

#[test]
fn lex_smoke_deterministic_mutations_do_not_panic() {
    let repo_root = repo_root();
    let mut seed = MUTATION_SEED;
    let mut failures = Vec::new();
    let mut mutation_count = 0;

    for path in sql_suite_files(&repo_root) {
        for payload in sql_payloads(&repo_root, &path) {
            let tokens = match lex(&payload.sql) {
                Ok(tokens) => tokens,
                Err(error) => {
                    failures.push(format!(
                        "{}: cannot construct token-level mutations because SQL payload lex failed: {error:?}",
                        payload.label
                    ));
                    continue;
                }
            };

            let mut mutations = Vec::with_capacity(3);
            mutations.push((
                "char-boundary truncation",
                truncate_at_char_boundary(&payload.sql, &mut seed),
            ));
            if let Some(token) = pick_non_end_token(&tokens, &mut seed) {
                mutations.push((
                    "token replacement",
                    replace_token(
                        &payload.sql,
                        token.span.start(),
                        token.span.end(),
                        &mut seed,
                    ),
                ));
                mutations.push((
                    "token deletion",
                    delete_token(&payload.sql, token.span.start(), token.span.end()),
                ));
            }

            for (kind, mutated) in mutations {
                mutation_count += 1;
                if catch_unwind(AssertUnwindSafe(|| lex(&mutated))).is_err() {
                    failures.push(format!("{}: {kind} panicked", payload.label));
                }
            }
        }
    }

    assert!(mutation_count > 0, "the mutation sweep generated no inputs");
    assert!(
        failures.is_empty(),
        "lex-smoke mutation failures (seed {MUTATION_SEED:#x}):\n{}",
        failures.join("\n")
    );
}

fn repo_root() -> PathBuf {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .ancestors()
        .find(|ancestor| ancestor.join("tests/sql/suites").is_dir())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| {
            panic!(
                "could not locate repository root by walking up from {}",
                manifest_dir.display()
            )
        })
}

fn sql_suite_files(repo_root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_sql_files(&repo_root.join("tests/sql/suites"), &mut files);
    files.sort();
    files
}

fn collect_sql_files(directory: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", directory.display()))
    {
        let entry = entry.unwrap_or_else(|error| {
            panic!(
                "failed to enumerate an entry below {}: {error}",
                directory.display()
            )
        });
        let path = entry.path();
        if path.is_dir() {
            collect_sql_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "sql") {
            files.push(path);
        }
    }
}

fn corpus_failures(repo_root: &Path, path: &Path) -> Vec<String> {
    sql_payloads(repo_root, path)
        .into_iter()
        .filter_map(|payload| match lex(&payload.sql) {
            Ok(tokens) => lossless_token_stream(&payload.sql, &tokens)
                .err()
                .map(|error| format!("{}: {error}", payload.label)),
            Err(error) => Some(format!(
                "{}: SQL payload lex failed: {error:?}",
                payload.label
            )),
        })
        .collect()
}

struct SqlPayload {
    label: String,
    sql: String,
}

fn sql_payloads(repo_root: &Path, path: &Path) -> Vec<SqlPayload> {
    let relative = path
        .strip_prefix(repo_root)
        .expect("SQL suite file must be below the repository root")
        .display()
        .to_string();
    let source = fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
    let lines = source.lines().collect::<Vec<_>>();
    let markers = lines
        .iter()
        .enumerate()
        .filter_map(|(index, line)| query_number(line).map(|number| (index, number)))
        .collect::<Vec<_>>();
    let sections = if markers.len() > 1
        && markers
            .iter()
            .enumerate()
            .all(|(index, (_, number))| *number == index + 1)
    {
        markers
            .iter()
            .enumerate()
            .map(|(index, (start, number))| {
                let end = markers
                    .get(index + 1)
                    .map_or(lines.len(), |(next, _)| *next);
                (*number, lines[*start..end].join("\n"))
            })
            .collect()
    } else {
        vec![(1, source)]
    };

    sections
        .into_iter()
        .filter(|(_, section)| !is_shell_payload(section))
        .map(|(number, sql)| SqlPayload {
            label: format!("{relative} (query {number})"),
            sql,
        })
        .collect()
}

fn query_number(line: &str) -> Option<usize> {
    let body = line.trim().strip_prefix("--")?.trim_start();
    let number = body.strip_prefix("query")?.trim();
    (!number.is_empty()).then(|| number.parse().ok()).flatten()
}

fn is_shell_payload(section: &str) -> bool {
    section
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with("--"))
        .is_some_and(|line| line.starts_with("shell:"))
}

fn lossless_token_stream(source: &str, tokens: &[novarocks_parser::Token]) -> Result<(), String> {
    let mut reconstructed = String::with_capacity(source.len());
    let mut cursor = 0;

    for (index, token) in tokens.iter().enumerate() {
        if token.kind == TokenKind::End {
            if index + 1 != tokens.len() {
                return Err(format!("End token at index {index} is not final"));
            }
            if token.span.start() != source.len() || token.span.end() != source.len() {
                return Err(format!(
                    "End token span {}..{} does not point at EOF {}",
                    token.span.start(),
                    token.span.end(),
                    source.len()
                ));
            }
            continue;
        }

        let start = token.span.start();
        let end = token.span.end();
        if start != cursor {
            return Err(format!(
                "token {index} starts at {start}, expected {cursor}"
            ));
        }
        if start > end || end > source.len() {
            return Err(format!(
                "token {index} has invalid span {start}..{end} for {} bytes",
                source.len()
            ));
        }
        if !source.is_char_boundary(start) || !source.is_char_boundary(end) {
            return Err(format!(
                "token {index} span {start}..{end} is not UTF-8 aligned"
            ));
        }
        reconstructed.push_str(&source[start..end]);
        cursor = end;
    }

    if cursor != source.len() {
        return Err(format!(
            "non-End token spans stop at {cursor}, expected EOF"
        ));
    }
    if reconstructed != source {
        return Err("non-End token spans do not reconstruct the source exactly".to_owned());
    }
    Ok(())
}

fn pick_non_end_token<'a>(
    tokens: &'a [novarocks_parser::Token],
    seed: &mut u64,
) -> Option<&'a novarocks_parser::Token> {
    let non_end = tokens
        .iter()
        .filter(|token| token.kind != TokenKind::End)
        .collect::<Vec<_>>();
    non_end.get(next_index(seed, non_end.len())).copied()
}

fn truncate_at_char_boundary(source: &str, seed: &mut u64) -> String {
    let boundaries = source
        .char_indices()
        .map(|(index, _)| index)
        .chain(std::iter::once(source.len()))
        .collect::<Vec<_>>();
    source[..boundaries[next_index(seed, boundaries.len())]].to_owned()
}

fn replace_token(source: &str, start: usize, end: usize, seed: &mut u64) -> String {
    const REPLACEMENTS: &[&str] = &["'", "/*", "`", "@", "?"];
    let replacement = REPLACEMENTS[next_index(seed, REPLACEMENTS.len())];
    format!("{}{}{}", &source[..start], replacement, &source[end..])
}

fn delete_token(source: &str, start: usize, end: usize) -> String {
    format!("{}{}", &source[..start], &source[end..])
}

fn next_index(seed: &mut u64, upper_bound: usize) -> usize {
    assert!(
        upper_bound > 0,
        "cannot choose from an empty mutation input"
    );
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    ((*seed >> 32) as usize) % upper_bound
}
