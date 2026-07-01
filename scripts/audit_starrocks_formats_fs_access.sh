#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

targets=(
  "src/formats/starrocks"
  "src/lower/node/hdfs_scan.rs"
  "src/lower/node/lake_scan.rs"
  "src/engine/iceberg_writer.rs"
)

existing_targets=()
for target in "${targets[@]}"; do
  if [[ -e "$target" ]]; then
    existing_targets+=("$target")
  fi
done

if [[ ${#existing_targets[@]} -eq 0 ]]; then
  exit 0
fi

pattern='(^|[^[:alnum:]_])(classify_scan_paths|ScanPathScheme|resolve_object_store_operator_and_path|resolve_opendal_paths|build_object_store_operator|build_oss_operator|normalize_oss_path|oss_config_for_path|opendal::services::Fs::default)([^[:alnum:]_]|$)'
mapfile -t hits < <(rg -n --no-heading "$pattern" "${existing_targets[@]}" || true)

is_test_line() {
  local file="$1"
  local line="$2"

  awk -v target="$line" '
    function brace_delta(text, copy, opens, closes) {
      copy = text
      opens = gsub(/\{/, "{", copy)
      copy = text
      closes = gsub(/\}/, "}", copy)
      return opens - closes
    }

    {
      if (in_tests) {
        if (NR == target) {
          found = 1
        }
        depth += brace_delta($0)
        if (depth <= 0) {
          in_tests = 0
        }
      }

      if (!in_tests && prev_cfg_test && $0 ~ /^[[:space:]]*mod[[:space:]]+tests[[:space:]]*\{/) {
        in_tests = 1
        depth = brace_delta($0)
        if (NR == target) {
          found = 1
        }
        if (depth <= 0) {
          in_tests = 0
        }
      }

      prev_cfg_test = ($0 ~ /^[[:space:]]*#\[cfg\(test\)\][[:space:]]*$/)
    }

    END {
      exit found ? 0 : 1
    }
  ' "$file"
}

blocked=()
for hit in "${hits[@]}"; do
  IFS=: read -r file line text <<<"$hit"

  if [[ "$file" == "src/formats/starrocks/fs_access.rs" ]]; then
    continue
  fi

  if is_test_line "$file" "$line"; then
    continue
  fi

  # Existing standalone Iceberg abort cleanup local-FS fallback is outside the
  # StarRocks formats file-access boundary. New direct local-FS sites still fail.
  if [[ "$file" == "src/engine/iceberg_writer.rs" ]]; then
    case "$text" in
      *"let builder = opendal::services::Fs::default().root(\"/\");"*)
        continue
        ;;
    esac
  fi

  blocked+=("$hit")
done

if [[ ${#blocked[@]} -ne 0 ]]; then
  printf 'FS-6 file-access boundary violations:\n' >&2
  printf '%s\n' "${blocked[@]}" >&2
  exit 1
fi
