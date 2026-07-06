//! Architecture guards for the plan-IR layering arc (PIR-8).
//!
//! These tests mechanically enforce the PIR import and stage boundaries. Test
//! modules may still build optimizer trees as inputs; production code may not
//! leak optimizer physical types into planner/codegen main paths.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

fn manifest_dir() -> &'static str {
    env!("CARGO_MANIFEST_DIR")
}

fn src_dir() -> PathBuf {
    Path::new(manifest_dir()).join("src")
}

fn rs_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(rs_files(&path));
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

fn rel(path: &Path) -> String {
    path.strip_prefix(manifest_dir())
        .unwrap_or(path)
        .display()
        .to_string()
}

fn brace_delta(line: &str) -> isize {
    line.chars().fold(0, |delta, ch| match ch {
        '{' => delta + 1,
        '}' => delta - 1,
        _ => delta,
    })
}

fn is_comment_or_blank(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.is_empty()
        || trimmed.starts_with("//")
        || trimmed.starts_with("/*")
        || trimmed.starts_with('*')
}

fn non_comment_trimmed_lines(text: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut in_block_comment = false;

    for line in text.lines() {
        let trimmed = line.trim();
        if in_block_comment {
            if trimmed.contains("*/") {
                in_block_comment = false;
            }
            continue;
        }

        if trimmed.is_empty() || trimmed.starts_with("//") || trimmed.starts_with('*') {
            continue;
        }
        if trimmed.starts_with("/*") {
            if !trimmed.contains("*/") {
                in_block_comment = true;
            }
            continue;
        }

        lines.push(trimmed);
    }

    lines
}

fn has_non_comment_line(text: &str, needle: &str) -> bool {
    non_comment_trimmed_lines(text)
        .into_iter()
        .any(|line| line == needle)
}

fn has_cfg_test_mod_tests(text: &str) -> bool {
    non_comment_trimmed_lines(text)
        .windows(2)
        .any(|lines| lines == ["#[cfg(test)]", "mod tests;"])
}

fn module_declarations(text: &str) -> BTreeSet<String> {
    non_comment_trimmed_lines(text)
        .into_iter()
        .filter_map(|line| {
            let declaration = line.strip_suffix(';')?;
            let module = declaration
                .strip_prefix("mod ")
                .or_else(|| declaration.strip_prefix("pub mod "))
                .or_else(|| declaration.strip_prefix("pub(crate) mod "))
                .or_else(|| declaration.strip_prefix("pub(super) mod "))?;
            if module
                .chars()
                .all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
            {
                Some(module.to_string())
            } else {
                None
            }
        })
        .collect()
}

fn has_module_declaration(text: &str, module: &str) -> bool {
    module_declarations(text).contains(module)
}

fn non_test_line_hits<F>(path: &Path, mut predicate: F) -> Vec<(usize, String)>
where
    F: FnMut(&str) -> bool,
{
    let text = fs::read_to_string(path).unwrap_or_default();
    let mut hits = Vec::new();
    let mut pending_cfg_test = false;
    let mut test_depth = 0isize;

    for (idx, line) in text.lines().enumerate() {
        let trimmed = line.trim_start();

        if test_depth > 0 {
            test_depth += brace_delta(line);
            if test_depth < 0 {
                test_depth = 0;
            }
            continue;
        }

        if trimmed.starts_with("#[cfg(test") {
            pending_cfg_test = true;
            let delta = brace_delta(line);
            if delta > 0 {
                test_depth = delta;
                pending_cfg_test = false;
            }
            continue;
        }

        if pending_cfg_test {
            let delta = brace_delta(line);
            if delta > 0 {
                test_depth = delta;
            }
            pending_cfg_test = false;
            continue;
        }

        if !is_comment_or_blank(line) && predicate(line) {
            hits.push((idx + 1, line.trim().to_string()));
        }
    }

    hits
}

fn source_line_hits<F>(path: &Path, mut predicate: F) -> Vec<(usize, String)>
where
    F: FnMut(&str) -> bool,
{
    let text = fs::read_to_string(path).unwrap_or_default();
    text.lines()
        .enumerate()
        .filter_map(|(idx, line)| {
            if !is_comment_or_blank(line) && predicate(line) {
                Some((idx + 1, line.trim().to_string()))
            } else {
                None
            }
        })
        .collect()
}

fn non_test_optimizer_refs(path: &Path) -> Vec<(usize, String)> {
    non_test_line_hits(path, |line| line.contains("crate::sql::optimizer::"))
}

fn test_dir() -> PathBuf {
    Path::new(manifest_dir()).join("tests")
}

fn source_and_test_rs_files() -> Vec<PathBuf> {
    let mut files = rs_files(&src_dir());
    files.extend(rs_files(&test_dir()));
    files
        .into_iter()
        .filter(|path| rel(path) != "tests/architecture_guard.rs")
        .collect()
}

fn rs_files_under(relative_roots: &[&str]) -> Vec<PathBuf> {
    let repo = Path::new(manifest_dir());
    let mut files = Vec::new();
    for root in relative_roots {
        files.extend(rs_files(&repo.join(root)));
    }
    files
}

fn is_ident_char(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

#[derive(Clone, Copy)]
enum RustWirePolicy {
    StrictNoWire,
    StarRocksProtoOnly,
    StrictNoStarRocksWire,
    AllowNativeProto,
    PlannerPartitionBridge,
}

#[derive(Clone, Copy, Default)]
struct RustWireContext {
    in_crate_use_group: bool,
    in_proto_use_group: bool,
    in_thrift_use_group: bool,
}

fn compact_line(line: &str) -> String {
    line.chars().filter(|ch| !ch.is_whitespace()).collect()
}

fn first_ident(text: &str) -> Option<String> {
    let start = text.find(|ch| is_ident_char(ch))?;
    let tail = &text[start..];
    let end = tail.find(|ch| !is_ident_char(ch)).unwrap_or(tail.len());
    Some(tail[..end].to_string())
}

fn group_entry_modules(text: &str) -> Vec<String> {
    text.split(',')
        .filter_map(first_ident)
        .filter(|entry| !matches!(entry.as_str(), "use" | "crate" | "self" | "super"))
        .collect()
}

fn modules_after_needle(compact: &str, needle: &str) -> Vec<String> {
    let mut modules = Vec::new();
    let mut rest = compact;
    while let Some(pos) = rest.find(needle) {
        let after = &rest[pos + needle.len()..];
        if let Some(group) = after.strip_prefix('{') {
            let end = group.find('}').unwrap_or(group.len());
            modules.extend(group_entry_modules(&group[..end]));
        } else if let Some(module) = first_ident(after) {
            modules.push(module);
        }
        rest = &after[after.len().min(1)..];
    }
    modules
}

fn line_has_ident(line: &str, ident: &str) -> bool {
    line.match_indices(ident).any(|(idx, _)| {
        let before = line[..idx].chars().next_back();
        let after = line[idx + ident.len()..].chars().next();
        before.is_none_or(|ch| !is_ident_char(ch)) && after.is_none_or(|ch| !is_ident_char(ch))
    })
}

fn proto_reference_modules(line: &str, context: RustWireContext) -> Vec<String> {
    let compact = compact_line(line);
    let in_crate_group = context.in_crate_use_group || compact.contains("crate::{");
    let mut modules = modules_after_needle(&compact, "crate::proto::");
    modules.extend(modules_after_needle(&compact, "grpc_client::proto::"));
    modules.extend(modules_after_needle(
        &compact,
        "service::grpc_client::proto::",
    ));
    if in_crate_group {
        modules.extend(modules_after_needle(&compact, "proto::"));
        if line_has_ident(line, "proto") && !compact.contains("proto::") {
            modules.push("proto".to_string());
        }
    }
    if context.in_proto_use_group {
        modules.extend(group_entry_modules(&compact));
    }
    modules.sort();
    modules.dedup();
    modules
}

fn thrift_reference_modules(line: &str, context: RustWireContext) -> Vec<String> {
    let compact = compact_line(line);
    let in_crate_group = context.in_crate_use_group || compact.contains("crate::{");
    let mut modules = modules_after_needle(&compact, "crate::thrift::");
    if in_crate_group {
        modules.extend(modules_after_needle(&compact, "thrift::"));
        if line_has_ident(line, "thrift") && !compact.contains("thrift::") {
            modules.push("thrift".to_string());
        }
    }
    if context.in_thrift_use_group {
        modules.extend(group_entry_modules(&compact));
    }
    if compact.contains("crate::types::arrow_thrift")
        || (in_crate_group && compact.contains("types::arrow_thrift"))
    {
        modules.push("arrow_thrift".to_string());
    }
    modules.sort();
    modules.dedup();
    modules
}

fn contains_starrocks_proto_ref(line: &str) -> bool {
    proto_reference_modules(line, RustWireContext::default())
        .iter()
        .any(|module| module == "starrocks")
}

fn contains_staros_proto_ref(line: &str) -> bool {
    proto_reference_modules(line, RustWireContext::default())
        .iter()
        .any(|module| module == "staros")
}

fn contains_thrift_ref(line: &str) -> bool {
    !thrift_reference_modules(line, RustWireContext::default()).is_empty()
}

fn rust_wire_policy_violates_line(
    line: &str,
    context: RustWireContext,
    policy: RustWirePolicy,
) -> bool {
    let proto_modules = proto_reference_modules(line, context);
    let thrift_modules = thrift_reference_modules(line, context);
    let starrocks_proto = proto_modules.iter().any(|module| module == "starrocks");
    let staros_proto = proto_modules.iter().any(|module| module == "staros");
    let thrift = !thrift_modules.is_empty();

    match policy {
        RustWirePolicy::StrictNoWire => !proto_modules.is_empty() || thrift,
        RustWirePolicy::StarRocksProtoOnly => starrocks_proto || staros_proto,
        RustWirePolicy::StrictNoStarRocksWire | RustWirePolicy::AllowNativeProto => {
            starrocks_proto || staros_proto || thrift
        }
        RustWirePolicy::PlannerPartitionBridge => {
            starrocks_proto
                || staros_proto
                || (thrift && thrift_modules.iter().any(|module| module != "partitions"))
        }
    }
}

fn update_wire_group_depth(depth: &mut isize, line: &str) {
    if *depth > 0 {
        *depth += brace_delta(line);
        if *depth < 0 {
            *depth = 0;
        }
    }
}

fn start_wire_group_depth(depth: &mut isize, line: &str, needle: &str) {
    if *depth == 0 && compact_line(line).contains(needle) {
        *depth = brace_delta(line).max(0);
    }
}

fn rust_wire_reference_hits(path: &Path, policy: RustWirePolicy) -> Vec<(usize, String)> {
    let text = fs::read_to_string(path).unwrap_or_default();
    let mut hits = Vec::new();
    let mut pending_cfg_test = false;
    let mut test_depth = 0isize;
    let mut crate_use_group_depth = 0isize;
    let mut proto_use_group_depth = 0isize;
    let mut thrift_use_group_depth = 0isize;

    for (idx, line) in text.lines().enumerate() {
        let trimmed = line.trim_start();

        if test_depth > 0 {
            test_depth += brace_delta(line);
            if test_depth < 0 {
                test_depth = 0;
            }
            continue;
        }

        if trimmed.starts_with("#[cfg(test") {
            pending_cfg_test = true;
            let delta = brace_delta(line);
            if delta > 0 {
                test_depth = delta;
                pending_cfg_test = false;
            }
            continue;
        }

        if pending_cfg_test {
            let delta = brace_delta(line);
            if delta > 0 {
                test_depth = delta;
            }
            pending_cfg_test = false;
            continue;
        }

        let context = RustWireContext {
            in_crate_use_group: crate_use_group_depth > 0,
            in_proto_use_group: proto_use_group_depth > 0,
            in_thrift_use_group: thrift_use_group_depth > 0,
        };
        if !is_comment_or_blank(line) && rust_wire_policy_violates_line(line, context, policy) {
            hits.push((idx + 1, line.trim().to_string()));
        }

        update_wire_group_depth(&mut crate_use_group_depth, line);
        update_wire_group_depth(&mut proto_use_group_depth, line);
        update_wire_group_depth(&mut thrift_use_group_depth, line);
        start_wire_group_depth(&mut crate_use_group_depth, line, "usecrate::{");
        start_wire_group_depth(&mut proto_use_group_depth, line, "usecrate::proto::{");
        start_wire_group_depth(&mut thrift_use_group_depth, line, "usecrate::thrift::{");
    }

    hits
}

fn proto_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(proto_files(&path));
            } else if path.extension().is_some_and(|ext| ext == "proto") {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

fn proto_imports(path: &Path) -> Vec<(usize, String)> {
    let text = fs::read_to_string(path).unwrap_or_default();
    let mut imports = Vec::new();
    for (idx, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("import ") {
            let rest = rest
                .strip_prefix("public ")
                .or_else(|| rest.strip_prefix("weak "))
                .unwrap_or(rest);
            if let Some(rest) = rest.strip_prefix('"')
                && let Some((import, _)) = rest.split_once('"')
            {
                imports.push((idx + 1, import.to_string()));
            }
        }
    }
    imports
}

fn disallowed_novarocks_proto_imports(files: &[PathBuf]) -> Vec<String> {
    let allowed = [
        "common.proto",
        "expr.proto",
        "filter.proto",
        "plan.proto",
        "service.proto",
    ];
    let mut hits = Vec::new();
    for file in files {
        for (line, import) in proto_imports(file) {
            if !allowed.contains(&import.as_str()) {
                hits.push(format!("{}:{}: import \"{}\"", rel(file), line, import));
            }
        }
    }
    hits
}

fn named_let_array_lines<'a>(text: &'a str, name: &str) -> Option<Vec<(usize, &'a str)>> {
    let lines = text.lines().collect::<Vec<_>>();
    let start = lines
        .iter()
        .position(|line| line.contains(&format!("let {name} = [")))?;
    let mut block = Vec::new();
    for (idx, line) in lines.iter().enumerate().skip(start) {
        block.push((idx + 1, *line));
        if line.contains("];") {
            return Some(block);
        }
    }
    Some(block)
}

fn compile_protos_call_lines<'a>(
    text: &'a str,
    protos_name: &str,
) -> Option<Vec<(usize, &'a str)>> {
    let lines = text.lines().collect::<Vec<_>>();
    let start = lines
        .iter()
        .position(|line| compact_line(line).contains(&format!("compile_protos(&{protos_name}")))?;
    let mut call = Vec::new();
    for (idx, line) in lines.iter().enumerate().skip(start).take(12) {
        call.push((idx + 1, *line));
        if line.contains(';') || line.contains(".unwrap()") || line.contains(".context(") {
            break;
        }
    }
    Some(call)
}

fn contains_compat_proto_root(line: &str) -> bool {
    line.contains("COMPAT_PROTO_DIR")
        || line.contains("COMPAT_STAROS_DIR")
        || line.contains("compat/proto")
        || line.contains("compat/staros")
}

fn block_contains(lines: &[(usize, &str)], needle: &str) -> bool {
    lines.iter().any(|(_, line)| line.contains(needle))
}

fn native_proto_codegen_boundary_violations(build_rs: &Path) -> Vec<String> {
    let text = fs::read_to_string(build_rs).unwrap_or_default();
    let mut hits = Vec::new();
    let build_rel = rel(build_rs);

    if let Some(native_block) = named_let_array_lines(&text, "novarocks_protos") {
        for (line, text) in &native_block {
            if contains_compat_proto_root(text) {
                hits.push(format!(
                    "{build_rel}:{line}: novarocks_protos must not include compat proto dirs: {}",
                    text.trim()
                ));
            }
        }
    } else {
        hits.push(format!("{build_rel}:1: novarocks_protos block must exist"));
    }

    if let Some(native_call) = compile_protos_call_lines(&text, "novarocks_protos") {
        let mut call_has_compat_root = false;
        for (line, text) in &native_call {
            if contains_compat_proto_root(text) {
                call_has_compat_root = true;
                hits.push(format!(
                    "{build_rel}:{line}: native compile_protos include roots must stay NOVAROCKS_IDL_DIR only: {}",
                    text.trim()
                ));
            }
        }
        let compact_call = native_call
            .iter()
            .map(|(_, line)| compact_line(line))
            .collect::<String>();
        if !call_has_compat_root
            && !compact_call.contains("compile_protos(&novarocks_protos,&[NOVAROCKS_IDL_DIR])")
        {
            let line = native_call.first().map(|(line, _)| *line).unwrap_or(1);
            hits.push(format!(
                "{build_rel}:{line}: native compile_protos include roots must be &[NOVAROCKS_IDL_DIR]"
            ));
        }
    } else {
        hits.push(format!(
            "{build_rel}:1: native compile_protos call for novarocks_protos must exist"
        ));
    }

    if let Some(starrocks_block) = named_let_array_lines(&text, "starrocks_protos") {
        if !block_contains(&starrocks_block, "COMPAT_PROTO_DIR") {
            let line = starrocks_block.first().map(|(line, _)| *line).unwrap_or(1);
            hits.push(format!(
                "{build_rel}:{line}: starrocks_protos must explicitly use COMPAT_PROTO_DIR"
            ));
        }
    } else {
        hits.push(format!("{build_rel}:1: starrocks_protos block must exist"));
    }

    if let Some(staros_block) = named_let_array_lines(&text, "staros_protos") {
        if !block_contains(&staros_block, "COMPAT_STAROS_DIR") {
            let line = staros_block.first().map(|(line, _)| *line).unwrap_or(1);
            hits.push(format!(
                "{build_rel}:{line}: staros_protos must explicitly use COMPAT_STAROS_DIR"
            ));
        }
    } else {
        hits.push(format!("{build_rel}:1: staros_protos block must exist"));
    }
    hits
}

#[test]
fn nidl_d2c_detector_flags_proto_build_and_rust_wire_violations() {
    let tmp_dir = std::env::temp_dir().join(format!(
        "nidl_d2c_guard_probe_{}_{}",
        std::process::id(),
        "wire_refs"
    ));
    fs::create_dir_all(&tmp_dir).unwrap();

    let proto = tmp_dir.join("service.proto");
    fs::write(
        &proto,
        concat!(
            "syntax = \"proto3\";\n",
            "import \"common.proto\";\n",
            "import \"../compat/proto/internal_service.proto\";\n",
            "import \"staros/starlet.proto\";\n",
            "import public \"../compat/proto/public.proto\";\n",
            "import weak \"staros/weak.proto\";\n",
        ),
    )
    .unwrap();
    let proto_hits = disallowed_novarocks_proto_imports(&[proto.clone()]);
    assert_eq!(proto_hits.len(), 4, "{proto_hits:?}");

    let build_rs = tmp_dir.join("build.rs");
    fs::write(
        &build_rs,
        concat!(
            "let novarocks_protos = [idl_path(NOVAROCKS_IDL_DIR, \"service.proto\"), idl_path(COMPAT_PROTO_DIR, \"internal_service.proto\")];\n",
            "tonic_build::configure().compile_protos(&novarocks_protos, &[NOVAROCKS_IDL_DIR, COMPAT_PROTO_DIR]).unwrap();\n",
            "let starrocks_protos = [idl_path(COMPAT_PROTO_DIR, \"internal_service.proto\")];\n",
            "tonic_build::configure().compile_protos(&starrocks_protos, &[COMPAT_PROTO_DIR]).unwrap();\n",
            "let staros_protos = [idl_path(COMPAT_STAROS_DIR, \"starlet.proto\")];\n",
            "tonic_build::configure().compile_protos(&staros_protos, &[COMPAT_STAROS_DIR]).unwrap();\n",
        ),
    )
    .unwrap();
    let build_hits = native_proto_codegen_boundary_violations(&build_rs);
    assert_eq!(build_hits.len(), 2, "{build_hits:?}");
    assert!(
        build_hits.iter().all(|hit| hit.contains("build.rs:")),
        "{build_hits:?}"
    );

    let rust = tmp_dir.join("planner.rs");
    fs::write(
        &rust,
        concat!(
            "use crate::proto::starrocks::PPlanFragment;\n",
            "use crate::proto::staros::StarStatus;\n",
            "use crate::thrift::types;\n",
            "use crate::thrift::partitions;\n",
            "use crate::{runtime, thrift::types};\n",
            "use crate::thrift::partitions; use crate::thrift::exprs;\n",
            "use crate::service::grpc_client::proto::starrocks::PPlanFragment;\n",
        ),
    )
    .unwrap();
    let strict_hits = rust_wire_reference_hits(&rust, RustWirePolicy::StrictNoStarRocksWire);
    assert_eq!(strict_hits.len(), 7, "{strict_hits:?}");
    let planner_hits = rust_wire_reference_hits(&rust, RustWirePolicy::PlannerPartitionBridge);
    assert_eq!(planner_hits.len(), 6, "{planner_hits:?}");
    assert!(contains_starrocks_proto_ref(
        "use crate::proto::{starrocks};"
    ));
    assert!(contains_staros_proto_ref("use crate::proto::{staros};"));
    assert!(contains_thrift_ref("use crate::{runtime, thrift::types};"));

    let common = tmp_dir.join("common.rs");
    fs::write(
        &common,
        concat!(
            "use crate::{runtime, proto::plan};\n",
            "use crate::proto::{common, plan};\n",
            "use crate::service::grpc_client::proto::starrocks::PPlanFragment;\n",
            "use crate::{\n",
            "    runtime,\n",
            "    thrift::types,\n",
            "};\n",
        ),
    )
    .unwrap();
    let common_hits = rust_wire_reference_hits(&common, RustWirePolicy::StrictNoWire);
    assert_eq!(common_hits.len(), 4, "{common_hits:?}");
    let proto_only_hits = rust_wire_reference_hits(&common, RustWirePolicy::StarRocksProtoOnly);
    assert_eq!(proto_only_hits.len(), 1, "{proto_only_hits:?}");

    fs::remove_dir_all(&tmp_dir).ok();
}

#[test]
fn nidl_d2c_novarocks_proto_imports_stay_native_only() {
    let files = proto_files(&Path::new(manifest_dir()).join("idl/novarocks"));
    let violations = disallowed_novarocks_proto_imports(&files);
    assert!(
        violations.is_empty(),
        "idl/novarocks proto files must import only native proto files:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d2c_native_proto_codegen_root_excludes_compat_idl() {
    let build_rs = Path::new(manifest_dir()).join("src/build.rs");
    let violations = native_proto_codegen_boundary_violations(&build_rs);
    assert!(
        violations.is_empty(),
        "native proto codegen must stay rooted at idl/novarocks, with StarRocks protos generated explicitly:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d2c_rust_wire_imports_stay_inside_owned_boundaries() {
    let mut violations = Vec::new();

    for file in rs_files_under(&["src/sql/analyzer", "src/sql/optimizer"]) {
        for (line, text) in rust_wire_reference_hits(&file, RustWirePolicy::StrictNoStarRocksWire) {
            violations.push(format!("{}:{}: {}", rel(&file), line, text));
        }
    }

    for file in rs_files_under(&["src/sql/planner"]) {
        for (line, text) in rust_wire_reference_hits(&file, RustWirePolicy::PlannerPartitionBridge)
        {
            violations.push(format!("{}:{}: {}", rel(&file), line, text));
        }
    }

    for file in rs_files_under(&["src/sql/codegen/proto_encode"]) {
        for (line, text) in rust_wire_reference_hits(&file, RustWirePolicy::StarRocksProtoOnly) {
            violations.push(format!("{}:{}: {}", rel(&file), line, text));
        }
    }

    for file in rs_files_under(&["src/lower/novarocks"]) {
        for (line, text) in rust_wire_reference_hits(&file, RustWirePolicy::AllowNativeProto) {
            violations.push(format!("{}:{}: {}", rel(&file), line, text));
        }
    }

    for file in rs_files_under(&["src/lower/common"]) {
        for (line, text) in rust_wire_reference_hits(&file, RustWirePolicy::StrictNoWire) {
            violations.push(format!("{}:{}: {}", rel(&file), line, text));
        }
    }

    assert!(
        violations.is_empty(),
        "D2C Rust wire imports crossed native/planner/lowering ownership boundaries:\n{}",
        violations.join("\n")
    );
}

#[test]
fn detector_flags_non_test_and_skips_cfg_test_blocks() {
    let tmp = std::env::temp_dir().join(format!(
        "pir8_guard_probe_{}_{}.rs",
        std::process::id(),
        "optimizer_refs"
    ));
    fs::write(
        &tmp,
        "\
use crate::sql::optimizer::operator::AggMode;
#[cfg(test)]
mod tests {
    use crate::sql::optimizer::operator::TopNPhase;
    fn fixture() { let _ = crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(); }
}
fn prod() { let _ = crate::sql::optimizer::property::DistributionSpec::Any; }
",
    )
    .unwrap();
    let hits = non_test_optimizer_refs(&tmp);
    fs::remove_file(&tmp).ok();

    assert_eq!(
        hits,
        vec![
            (
                1,
                "use crate::sql::optimizer::operator::AggMode;".to_string()
            ),
            (
                7,
                "fn prod() { let _ = crate::sql::optimizer::property::DistributionSpec::Any; }"
                    .to_string()
            ),
        ]
    );
}

#[test]
fn nidl_d3a_detector_ignores_commented_module_declarations() {
    let commented = "\
// mod proto_contract;
/*
mod proto_contract;
*/
/*
#[cfg(test)]
mod tests;
*/
";
    assert!(!has_non_comment_line(commented, "mod proto_contract;"));
    assert!(!has_cfg_test_mod_tests(commented));
    assert!(module_declarations(commented).is_empty());

    let active = "\
#[cfg(test)]
// comment between attribute and module
mod tests;
mod proto_contract;
pub(crate) mod chunk;
";
    assert!(has_cfg_test_mod_tests(active));
    assert!(has_non_comment_line(active, "mod proto_contract;"));
    assert_eq!(
        module_declarations(active),
        BTreeSet::from([
            "chunk".to_string(),
            "proto_contract".to_string(),
            "tests".to_string()
        ])
    );
}

#[test]
fn planner_distributed_and_codegen_do_not_import_optimizer() {
    let mut checked = vec![
        src_dir().join("sql/planner/plan.rs"),
        src_dir().join("sql/planner/distributed_fragment.rs"),
        src_dir().join("sql/planner/distributed_node.rs"),
        src_dir().join("sql/planner/distributed_plan_build.rs"),
    ];
    checked.extend(rs_files(&src_dir().join("sql/codegen")));

    let mut violations = Vec::new();
    for file in &checked {
        for (line, text) in non_test_optimizer_refs(file) {
            violations.push(format!("{}:{}: {}", rel(file), line, text));
        }
    }

    assert!(
        violations.is_empty(),
        "planner distributed/codegen production paths must not reference optimizer types; \
         optimizer_bridge/** is the conversion boundary. Violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn optimizer_bridge_is_the_only_allowlisted_converter() {
    let bridge = src_dir().join("sql/planner/optimizer_bridge/physical.rs");
    assert!(bridge.exists(), "Bridge 2a must exist at {}", rel(&bridge));
    let text = fs::read_to_string(&bridge).unwrap();
    assert!(
        text.contains("crate::sql::optimizer"),
        "Bridge 2a should be the explicit optimizer-to-planner conversion boundary"
    );
}

#[test]
fn engine_has_no_direct_exec_resurrection() {
    let forbidden = [
        "collapse_distribution_enforcers_for_single_fragment",
        "DirectExecutionReason",
        "execute_query_direct_for_explicit_exception",
        "single_fragment_plan",
    ];
    let mut violations = Vec::new();

    for file in rs_files(&src_dir().join("engine")) {
        for symbol in forbidden {
            for (line, text) in non_test_line_hits(&file, |line| line.contains(symbol)) {
                violations.push(format!(
                    "{}:{}: forbidden direct-exec symbol `{}` in `{}`",
                    rel(&file),
                    line,
                    symbol,
                    text
                ));
            }
        }

        let rel_path = rel(&file);
        let optimizer_physical_allowlist = [
            "src/engine/query_stats.rs",
            "src/engine/dml_change_stream.rs",
            "src/engine/iceberg_change_stream_write.rs",
            "src/engine/mod.rs",
            "src/engine/mutation_flow.rs",
            "src/engine/mv/iceberg_refresh.rs",
        ];
        if !optimizer_physical_allowlist.contains(&rel_path.as_str()) {
            for (line, text) in non_test_line_hits(&file, |line| {
                line.contains("crate::sql::optimizer::physical_tree")
                    || line.contains("OptimizerPhysicalNode")
            }) {
                violations.push(format!(
                    "{}:{}: engine must not consume optimizer physical tree: {}",
                    rel(&file),
                    line,
                    text
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "engine direct-exec / optimizer-physical guard failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn stage_validation_guard_stays_deleted() {
    let mut violations = Vec::new();
    for file in rs_files(&src_dir().join("sql/planner")) {
        for (line, text) in non_test_line_hits(&file, |line| {
            line.contains("validate_logical_plan_stage")
                || line.contains("validate_physical_plan_stage")
        }) {
            violations.push(format!("{}:{}: {}", rel(&file), line, text));
        }
    }

    assert!(
        violations.is_empty(),
        "stage validation helpers must stay deleted; use type-level stage separation:\n{}",
        violations.join("\n")
    );
}

#[test]
fn build_distributed_plan_signature_is_planner_typed() {
    let path = src_dir().join("sql/planner/distributed_plan_build.rs");
    let text = fs::read_to_string(&path).unwrap();
    let sig = text
        .lines()
        .find(|line| line.contains("fn build_distributed_plan("))
        .expect("build_distributed_plan must exist");

    assert!(
        sig.contains("&PhysicalPlanNode") && !sig.contains("optimizer"),
        "build_distributed_plan must accept planner &PhysicalPlanNode, not optimizer types: {sig}"
    );
}

#[test]
fn distributed_plan_node_has_no_optimizer_payloads() {
    let file = src_dir().join("sql/planner/distributed_node.rs");
    let violations = non_test_optimizer_refs(&file)
        .into_iter()
        .map(|(line, text)| format!("{}:{}: {}", rel(&file), line, text))
        .collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "DistributedPlanNode must not contain optimizer paths:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d1_pure_mode_gates_starrocks_compat_behavior() {
    let repo = Path::new(manifest_dir());
    let service_mod = fs::read_to_string(repo.join("src/service/mod.rs")).unwrap();
    for module in [
        "backend_service",
        "heartbeat_service",
        "internal_service",
        "stream_load",
        "stream_load_http",
    ] {
        let expected = format!("#[cfg(feature = \"compat\")]\npub mod {module};");
        assert!(
            service_mod.contains(&expected),
            "service module `{module}` must be compat-gated"
        );
    }

    let grpc = fs::read_to_string(repo.join("src/service/grpc_server.rs")).unwrap();
    assert!(
        grpc.contains("#[cfg(feature = \"compat\")]\nfn build_novarocks_http_app"),
        "stream-load HTTP routes must only exist in compat grpc app"
    );
    assert!(
        grpc.contains(
            "#[cfg(feature = \"compat\")]\n#[derive(Default)]\npub struct StarletGrpcService"
        ),
        "Starlet gRPC service must be compat-gated"
    );
    assert!(
        grpc.contains("thrift SubmitFragment requires the compat feature"),
        "pure SubmitFragment must reject thrift fallback explicitly"
    );
}

#[test]
fn nidl_d2d_lowering_root_exposes_named_ownership_modules() {
    let repo = Path::new(manifest_dir());
    assert!(
        !repo.join(concat!("src/lower", "_native")).exists(),
        concat!(
            "src/lower",
            "_native must be deleted; native lowering lives under src/lower/novarocks"
        )
    );
    for dir in [
        "src/lower/common",
        "src/lower/compact",
        "src/lower/novarocks",
    ] {
        assert!(repo.join(dir).is_dir(), "{dir} must exist");
    }

    let lower_mod = fs::read_to_string(repo.join("src/lower/mod.rs")).unwrap();
    for expected in [
        "pub(crate) mod common;",
        "pub(crate) mod compact;",
        "pub(crate) mod novarocks;",
    ] {
        assert!(
            lower_mod.contains(expected),
            "src/lower/mod.rs must contain `{expected}`"
        );
    }
    for forbidden in [
        "pub(crate) mod expr;",
        "pub(crate) mod fragment;",
        "pub(crate) mod layout;",
        "pub(crate) mod node;",
        "pub(crate) mod sink;",
        "pub(crate) mod type_lowering;",
        "mod thrift",
        "pub(crate) mod thrift",
    ] {
        assert!(
            !lower_mod.contains(forbidden),
            "src/lower/mod.rs must not keep legacy direct module `{forbidden}`"
        );
    }
}

#[test]
fn nidl_d2d_legacy_lowering_paths_do_not_remain() {
    let forbidden = [
        concat!("crate::", "lower", "_native"),
        concat!("lower", "::thrift"),
        concat!("crate::lower", "::fragment"),
        concat!("crate::lower", "::expr"),
        concat!("crate::lower", "::layout"),
        concat!("crate::lower", "::node"),
        concat!("crate::lower", "::sink"),
        concat!("crate::lower", "::type_lowering"),
    ];

    let mut violations = Vec::new();
    for file in source_and_test_rs_files() {
        for needle in forbidden {
            for (line, text) in source_line_hits(&file, |line| line.contains(needle)) {
                violations.push(format!("{}:{}: {}", rel(&file), line, text));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "D2D lowering paths must use crate::lower::compact, crate::lower::novarocks, or crate::lower::common:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d2d_common_lowering_has_no_wire_dependencies() {
    let common_dir = src_dir().join("lower/common");
    let forbidden = [
        "native_fragment_wire",
        "crate::thrift",
        "crate::proto",
        "thrift::",
        "proto::",
    ];

    let mut violations = Vec::new();
    for file in rs_files(&common_dir) {
        for needle in forbidden {
            for (line, text) in source_line_hits(&file, |line| line.contains(needle)) {
                violations.push(format!("{}:{}: {}", rel(&file), line, text));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "src/lower/common must stay protocol-neutral and must not depend on thrift/proto/native wire adapters:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d3a_crate_internal_tests_live_under_src_tests() {
    let repo = Path::new(manifest_dir());
    let proto_contract_dir = repo.join("src/tests/proto_contract");
    let testutil_dir = repo.join("src/tests/testutil");
    let mut violations = Vec::new();
    if repo.join("src/proto_contract").exists() {
        violations.push(
            "src/proto_contract must not be a top-level src module; move it to src/tests/proto_contract"
                .to_string(),
        );
    }
    if repo.join("src/testutil").exists() {
        violations.push(
            "src/testutil must not be a top-level src module; move it to src/tests/testutil"
                .to_string(),
        );
    }
    if !repo.join("src/tests/mod.rs").is_file() {
        violations
            .push("src/tests/mod.rs must own crate-internal white-box test suites".to_string());
    }
    if !repo.join("src/tests/proto_contract/mod.rs").is_file() {
        violations.push(
            "src/tests/proto_contract/mod.rs must own native proto contract tests".to_string(),
        );
    }
    if !testutil_dir.join("mod.rs").is_file() {
        violations.push("src/tests/testutil/mod.rs must own test utility modules".to_string());
    }
    if !testutil_dir.join("chunk.rs").is_file() {
        violations
            .push("chunk test utilities must live at src/tests/testutil/chunk.rs".to_string());
    }

    for file in [
        "common.rs",
        "expr.rs",
        "filter.rs",
        "instance_params.rs",
        "plan.rs",
        "report.rs",
        "service.rs",
    ] {
        let path = proto_contract_dir.join(file);
        if !path.is_file() {
            violations.push(format!(
                "native proto contract test file must live at {}",
                rel(&path)
            ));
        }
    }

    let lib = fs::read_to_string(repo.join("src/lib.rs")).unwrap();
    if !has_cfg_test_mod_tests(&lib) {
        violations.push(
            "src/lib.rs must mount crate-internal white-box tests through #[cfg(test)] mod tests"
                .to_string(),
        );
    }
    if has_module_declaration(&lib, "proto_contract") {
        violations.push("src/lib.rs must not keep the legacy proto_contract module".to_string());
    }
    if has_module_declaration(&lib, "testutil") {
        violations.push("src/lib.rs must not keep the legacy testutil module".to_string());
    }

    if let Ok(root_mod) = fs::read_to_string(repo.join("src/tests/mod.rs")) {
        if !has_module_declaration(&root_mod, "proto_contract") {
            violations.push("src/tests/mod.rs must mount the proto contract suite".to_string());
        }
        if !has_module_declaration(&root_mod, "testutil") {
            violations.push("src/tests/mod.rs must mount test utility modules".to_string());
        }
    }

    if let Ok(testutil_mod) = fs::read_to_string(testutil_dir.join("mod.rs")) {
        if !has_module_declaration(&testutil_mod, "chunk") {
            violations
                .push("src/tests/testutil/mod.rs must mount chunk test utilities".to_string());
        }
    }

    if let Ok(proto_mod) = fs::read_to_string(proto_contract_dir.join("mod.rs")) {
        let declared_modules = module_declarations(&proto_mod);
        let mut file_modules = BTreeSet::new();
        if let Ok(entries) = fs::read_dir(&proto_contract_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == "rs")
                    && path.file_name().and_then(|name| name.to_str()) != Some("mod.rs")
                {
                    if let Some(module) = path.file_stem().and_then(|stem| stem.to_str()) {
                        file_modules.insert(module.to_string());
                    }
                }
            }
        }

        for module in &file_modules {
            if !declared_modules.contains(module) {
                violations.push(format!(
                    "src/tests/proto_contract/mod.rs must declare `mod {module};`"
                ));
            }
        }
        for module in &declared_modules {
            if !file_modules.contains(module) {
                violations.push(format!(
                    "src/tests/proto_contract/mod.rs declares `{module}`, but src/tests/proto_contract/{module}.rs is missing"
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "proto contract test layout guard failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d3a_test_contract_modules_do_not_leak_into_production_code() {
    let mut violations = Vec::new();
    for file in rs_files(&src_dir()) {
        let rel_path = rel(&file);
        if rel_path == "src/lib.rs" || rel_path.starts_with("src/tests/") {
            continue;
        }

        for (line, text) in non_test_line_hits(&file, |line| {
            line.contains("crate::tests") || line.contains("proto_contract")
        }) {
            violations.push(format!("{}:{}: {}", rel_path, line, text));
        }
    }

    assert!(
        violations.is_empty(),
        "test-only contract modules must not be referenced by production code:\n{}",
        violations.join("\n")
    );
}

#[test]
fn distributed_build_does_not_call_optimizer_cost_model() {
    let file = src_dir().join("sql/planner/distributed_plan_build.rs");
    let mut violations = Vec::new();
    for needle in ["compute_cost_estimate", "broadcast_decision("] {
        for (line, text) in non_test_line_hits(&file, |line| line.contains(needle)) {
            violations.push(format!("{}:{}: {}", rel(&file), line, text));
        }
    }

    assert!(
        violations.is_empty(),
        "distributed build must not call optimizer cost model:\n{}",
        violations.join("\n")
    );
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ProtoSchema {
    version: u32,
    files: BTreeMap<String, ProtoFileSchema>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ProtoFileSchema {
    package: String,
    messages: BTreeMap<String, ProtoMessageSchema>,
    enums: BTreeMap<String, ProtoEnumSchema>,
    services: BTreeMap<String, ProtoServiceSchema>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ProtoMessageSchema {
    fields: BTreeMap<u32, ProtoFieldSchema>,
    reserved_numbers: BTreeSet<u32>,
    reserved_names: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ProtoFieldSchema {
    number: u32,
    name: String,
    type_name: String,
    label: String,
    oneof: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ProtoEnumSchema {
    values: Vec<ProtoEnumValueSchema>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ProtoEnumValueSchema {
    number: i32,
    name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ProtoServiceSchema {
    rpcs: BTreeMap<String, ProtoRpcSchema>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ProtoRpcSchema {
    request: String,
    response: String,
    client_streaming: bool,
    server_streaming: bool,
}

fn parse_proto_schema(_path: &str, _input: &str) -> Result<ProtoFileSchema, String> {
    Err("not implemented".to_string())
}

#[test]
fn nidl_d3b_proto_schema_parser_handles_current_syntax() {
    let input = r#"
        syntax = "proto3";
        package novarocks.plan;

        message Outer {
          reserved 4, 6 to 8;
          reserved "old_name", "old_flag";
          optional string name = 1;
          repeated int64 ids = 2;
          map<int32, novarocks.plan.ScanRangeList> ranges = 3;
          oneof kind {
            bool enabled = 5;
          }
          enum InnerState {
            INNER_STATE_UNSPECIFIED = 0;
            INNER_STATE_READY = 1;
          }
        }

        service NovaRocksGrpc {
          rpc TransmitRuntimeFilter(novarocks.filter.TransmitRuntimeFilterRequest)
              returns (novarocks.filter.TransmitRuntimeFilterResponse);
          rpc Exchange(stream ExchangeRequest) returns (stream ExchangeResponse);
        }
    "#;

    let schema =
        parse_proto_schema("idl/novarocks/sample.proto", input).expect("sample proto should parse");
    assert_eq!(schema.package, "novarocks.plan");
    assert_eq!(schema.messages["Outer"].fields[&1].label, "optional");
    assert_eq!(schema.messages["Outer"].fields[&2].label, "repeated");
    assert_eq!(
        schema.messages["Outer"].fields[&3].type_name,
        "map<int32, novarocks.plan.ScanRangeList>"
    );
    assert_eq!(
        schema.messages["Outer"].fields[&5].oneof.as_deref(),
        Some("kind")
    );
    assert!(schema.messages["Outer"].reserved_numbers.contains(&4));
    assert!(schema.messages["Outer"].reserved_numbers.contains(&7));
    assert!(schema.messages["Outer"].reserved_names.contains("old_name"));
    assert_eq!(
        schema.enums["Outer.InnerState"].values[0].name,
        "INNER_STATE_UNSPECIFIED"
    );
    assert_eq!(
        schema.services["NovaRocksGrpc"].rpcs["TransmitRuntimeFilter"].request,
        "novarocks.filter.TransmitRuntimeFilterRequest"
    );
    assert_eq!(
        schema.services["NovaRocksGrpc"].rpcs["TransmitRuntimeFilter"].response,
        "novarocks.filter.TransmitRuntimeFilterResponse"
    );
    assert!(schema.services["NovaRocksGrpc"].rpcs["Exchange"].client_streaming);
    assert!(schema.services["NovaRocksGrpc"].rpcs["Exchange"].server_streaming);
}
