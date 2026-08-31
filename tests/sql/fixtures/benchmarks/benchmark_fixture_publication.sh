#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Direct S3 publication helpers.  The local storage mode exists only for the
# hermetic lifecycle tests; production always uses SigV4 curl requests.

fixture_publication_uri_path() {
  local uri="$1" root="${BENCHMARK_FIXTURE_STORAGE_DIR:?local storage root is required}"
  [[ "$uri" == s3://* ]] || return 64
  printf '%s/%s' "$root" "${uri#s3://}"
}

fixture_publication_curl_capable() {
  curl --help all 2>/dev/null | grep -q -- '--aws-sigv4'
}

fixture_publication_s3_url() {
  local uri="$1" endpoint="${AWS_S3_ENDPOINT:?AWS_S3_ENDPOINT is required}"
  [[ "$uri" == s3://* ]] || return 64
  printf '%s/%s' "${endpoint%/}" "${uri#s3://}"
}

fixture_publication_make_curl_config() {
  local config="$1" method="$2" uri="$3" conditional_header="${4:-}" body="${5:-}" query="${6:-}"
  umask 077
  : > "$config"
  chmod 600 "$config"
  {
    printf 'silent\nshow-error\nfail-with-body\nrequest = "%s"\n' "$method"
    printf 'url = "%s%s"\n' "$(fixture_publication_s3_url "$uri")" "$query"
    printf 'aws-sigv4 = "aws:amz:%s:s3"\n' "${AWS_REGION:-us-east-1}"
    # curl config keeps this credential out of argv and out of command logs.
    printf 'user = "%s:%s"\n' "${AWS_S3_ACCESS_KEY_ID:?AWS_S3_ACCESS_KEY_ID is required}" "${AWS_S3_SECRET_ACCESS_KEY:?AWS_S3_SECRET_ACCESS_KEY is required}"
    [[ -z "$conditional_header" ]] || printf 'header = "%s"\n' "$conditional_header"
    [[ -z "$body" ]] || printf 'data-binary = "@%s"\n' "$body"
  } >> "$config"
}

fixture_publication_list_prefix() {
  # stdout is one s3:// URI per object.  This is a direct S3 ListObjectsV2
  # operation, not a catalog or an mc/FE query.
  local prefix_uri="$1"
  if [[ -n "${BENCHMARK_FIXTURE_STORAGE_DIR:-}" ]]; then
    local path root
    path="$(fixture_publication_uri_path "$prefix_uri")"
    root="${BENCHMARK_FIXTURE_STORAGE_DIR%/}/"
    [[ -d "$path" ]] || return 44
    find "$path" -type f ! -name '*.etag' ! -name '*.conditional-lock' -print | while IFS= read -r object; do
      printf 's3://%s\n' "${object#"$root"}"
    done
    return
  fi
  fixture_publication_curl_capable || return 64
  local bucket key encoded_prefix config headers body status
  bucket="${prefix_uri#s3://}"; bucket="${bucket%%/*}"
  key="${prefix_uri#s3://$bucket/}"
  encoded_prefix="$(python3 - "$key" <<'PY'
import sys
from urllib.parse import quote
print(quote(sys.argv[1], safe='/'))
PY
)"
  config="$(mktemp)"; headers="$(mktemp)"; body="$(mktemp)"
  trap 'rm -f "$config" "$headers" "$body"' RETURN
  fixture_publication_make_curl_config "$config" GET "s3://$bucket" "" "" "?list-type=2&prefix=$encoded_prefix"
  status="$(curl --config "$config" --dump-header "$headers" --output "$body" --write-out '%{http_code}' 2>/dev/null || true)"
  [[ "$status" == 200 ]] || return 44
  python3 - "$bucket" "$body" <<'PY'
import sys
from xml.etree import ElementTree
bucket, source = sys.argv[1:]
root = ElementTree.parse(source).getroot()
for node in root.iter():
    if node.tag.rsplit('}', 1)[-1] == 'Key' and node.text:
        print(f's3://{bucket}/{node.text}')
PY
}

fixture_publication_get() {
  # Sets FIXTURE_PUBLICATION_BODY and FIXTURE_PUBLICATION_ETAG.  Do not invoke
  # this through command substitution: callers need the ETag from this shell.
  local uri="$1"
  if [[ -n "${BENCHMARK_FIXTURE_STORAGE_DIR:-}" ]]; then
    local path
    path="$(fixture_publication_uri_path "$uri")"
    [[ -f "$path" ]] || return 44
    FIXTURE_PUBLICATION_ETAG="$(cat "$path.etag" 2>/dev/null || sha256sum "$path" | awk '{print $1}')"
    FIXTURE_PUBLICATION_BODY="$(cat "$path")"
    return
  fi
  fixture_publication_curl_capable || return 64
  local config headers body status
  config="$(mktemp)"; headers="$(mktemp)"; body="$(mktemp)"
  trap 'rm -f "$config" "$headers" "$body"' RETURN
  fixture_publication_make_curl_config "$config" GET "$uri"
  status="$(curl --config "$config" --dump-header "$headers" --output "$body" --write-out '%{http_code}' 2>/dev/null || true)"
  [[ "$status" == 200 ]] || return 44
  FIXTURE_PUBLICATION_ETAG="$(awk 'BEGIN{IGNORECASE=1} /^etag:/ {gsub("\\r", ""); sub(/^[^:]*:[[:space:]]*/, ""); gsub(/\"/, ""); print; exit}' "$headers")"
  [[ -n "$FIXTURE_PUBLICATION_ETAG" ]] || return 1
  FIXTURE_PUBLICATION_BODY="$(cat "$body")"
}

fixture_publication_head() {
  local uri="$1"
  if [[ -n "${BENCHMARK_FIXTURE_STORAGE_DIR:-}" ]]; then
    [[ -f "$(fixture_publication_uri_path "$uri")" ]]
    return
  fi
  fixture_publication_curl_capable || return 64
  local config status
  config="$(mktemp)"; trap 'rm -f "$config"' RETURN
  fixture_publication_make_curl_config "$config" HEAD "$uri"
  status="$(curl --config "$config" --output /dev/null --write-out '%{http_code}' 2>/dev/null || true)"
  [[ "$status" == 200 ]]
}

fixture_publication_put_conditional() {
  # stdout is 200/201 or precondition status 409/412; never retries/blind-writes.
  local uri="$1" body="$2" mode="$3" observed_etag="${4:-}"
  local header
  case "$mode" in
    absent) header='If-None-Match: *' ;;
    rebuild) [[ -n "$observed_etag" ]] || return 64; header="If-Match: $observed_etag" ;;
    *) return 64 ;;
  esac
  if [[ -n "${BENCHMARK_FIXTURE_STORAGE_DIR:-}" ]]; then
    local path lock etag
    path="$(fixture_publication_uri_path "$uri")"; lock="$path.conditional-lock"
    mkdir -p "$(dirname "$path")"
    if ! mkdir "$lock" 2>/dev/null; then printf '409\n'; return; fi
    trap 'rmdir "$lock" 2>/dev/null || true' RETURN
    if [[ -f "$path.etag" ]]; then
      etag="$(cat "$path.etag")"
    elif [[ -f "$path" ]]; then
      etag="$(sha256sum "$path" | awk '{print $1}')"
    else
      etag=""
    fi
    if [[ "$mode" == absent && -e "$path" ]]; then printf '412\n'; return; fi
    if [[ "$mode" == rebuild && "$etag" != "$observed_etag" ]]; then printf '412\n'; return; fi
    cp "$body" "$path"; sha256sum "$path" | awk '{print $1}' > "$path.etag"; printf '200\n'; return
  fi
  fixture_publication_curl_capable || return 64
  local config status
  config="$(mktemp)"; trap 'rm -f "$config"' RETURN
  fixture_publication_make_curl_config "$config" PUT "$uri" "$header" "$body"
  status="$(curl --config "$config" --output /dev/null --write-out '%{http_code}' 2>/dev/null || true)"
  printf '%s\n' "$status"
}
