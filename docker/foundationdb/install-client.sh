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

set -euo pipefail

VERSION="7.3.69"
DARWIN_ASSET="FoundationDB-7.3.69_arm64.pkg"
DARWIN_SHA256="6bfbd48ac21356de0baa0c1e84c6e33d15d95d0b9d022c35a7625e5d9293b71e"
LINUX_ASSET="foundationdb-clients_7.3.69-1_amd64.deb"
LINUX_SHA256="ea59d1708519798c7bc4f514cd29af1ac8e41dccbec4371f22d86b713ea81cbf"
RELEASE_BASE_URL="https://github.com/apple/foundationdb/releases/download/${VERSION}"

usage() {
  echo "usage: $0 <runtime-directory>" >&2
}

sha256_file() {
  local path="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$path" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$path" | awk '{print $1}'
  else
    echo "sha256sum or shasum is required" >&2
    return 1
  fi
}

copy_or_download() {
  local source="$1"
  local destination="$2"
  local temporary="${destination}.tmp.$$"

  rm -f "$temporary"
  case "$source" in
    http://*|https://*)
      curl --fail --location --retry 3 --retry-delay 2 \
        --connect-timeout 30 --max-time 300 --show-error --silent \
        --output "$temporary" "$source"
      ;;
    *)
      test -f "$source" || {
        echo "FoundationDB client asset does not exist: $source" >&2
        return 1
      }
      cp "$source" "$temporary"
      ;;
  esac
  mv "$temporary" "$destination"
}

write_manifest() {
  local path="$1"
  local library_path="$2"
  local cli_path="$3"
  local library_dir
  library_dir="$(dirname "$library_path")"

  {
    printf 'export NOVA_FDB_CLIENT_PLATFORM=%q\n' "$platform"
    printf 'export NOVA_FDB_CLIENT_ASSET_PATH=%q\n' "$asset_path"
    printf 'export NOVA_FDB_CLIENT_ASSET_SHA256=%q\n' "$expected_sha256"
    printf 'export NOVA_FDB_CLIENT_LIBRARY_DIR=%q\n' "$library_dir"
    printf 'export NOVA_FDB_CLIENT_LIBRARY_FILE=%q\n' "$library_path"
    printf 'export FDB_CLIENT_LIB_PATH=%q\n' "$library_dir"
    printf 'export NOVA_FDB_FDBCLI=%q\n' "$cli_path"
  } > "$path"
}

if [[ "$#" -ne 1 ]]; then
  usage
  exit 2
fi

runtime_dir="$(mkdir -p "$1" && cd "$1" && pwd)"
downloads_dir="$runtime_dir/downloads"
client_dir="$runtime_dir/client"
manifest="$runtime_dir/client.env"
mkdir -p "$downloads_dir"

os="$(uname -s)"
arch="$(uname -m)"
case "$os/$arch" in
  Darwin/arm64)
    platform="darwin-arm64"
    asset_name="$DARWIN_ASSET"
    expected_sha256="$DARWIN_SHA256"
    ;;
  Linux/x86_64|Linux/amd64)
    platform="linux-x86_64"
    asset_name="$LINUX_ASSET"
    expected_sha256="$LINUX_SHA256"
    ;;
  *)
    echo "unsupported FoundationDB client platform: $os/$arch" >&2
    echo "supported platforms: Darwin/arm64 and Linux/x86_64" >&2
    exit 1
    ;;
esac

asset_path="$downloads_dir/$asset_name"
asset_source="${NOVA_FDB_CLIENT_ASSET:-$RELEASE_BASE_URL/$asset_name}"
if [[ ! -f "$asset_path" || -n "${NOVA_FDB_CLIENT_ASSET:-}" ]]; then
  copy_or_download "$asset_source" "$asset_path"
fi

actual_sha256="$(sha256_file "$asset_path")"
if [[ "$actual_sha256" != "$expected_sha256" ]]; then
  echo "FoundationDB client asset SHA-256 mismatch" >&2
  echo "asset: $asset_path" >&2
  echo "expected: $expected_sha256" >&2
  echo "actual: $actual_sha256" >&2
  exit 1
fi

rm -rf "$client_dir"
mkdir -p "$client_dir"

if [[ "$platform" == "linux-x86_64" ]]; then
  command -v dpkg-deb >/dev/null 2>&1 || {
    echo "dpkg-deb is required to extract the Linux FoundationDB client" >&2
    exit 1
  }
  dpkg-deb -x "$asset_path" "$client_dir/root"
  library_path="$(find "$client_dir/root" -type f \( -name 'libfdb_c.so' -o -name 'libfdb_c.so.*' \) -print | sort | head -1)"
  cli_path="$(find "$client_dir/root" -type f -name fdbcli -print | sort | head -1)"
else
  command -v pkgutil >/dev/null 2>&1 || {
    echo "pkgutil is required to extract the macOS FoundationDB client" >&2
    exit 1
  }
  command -v file >/dev/null 2>&1 || {
    echo "file is required to verify the macOS FoundationDB client architecture" >&2
    exit 1
  }
  pkgutil --expand-full "$asset_path" "$client_dir/expanded"
  library_path="$(find "$client_dir/expanded" -type f -name 'libfdb_c*.dylib' -print | sort | head -1)"
  cli_path="$(find "$client_dir/expanded" -type f -name fdbcli -print | sort | head -1)"

  if [[ -n "$library_path" ]] && ! file "$library_path" | grep -q 'arm64'; then
    echo "extracted FoundationDB client library is not arm64: $library_path" >&2
    exit 1
  fi
  if [[ -n "$cli_path" ]] && ! file "$cli_path" | grep -q 'arm64'; then
    echo "extracted fdbcli is not arm64: $cli_path" >&2
    exit 1
  fi

  chmod u+w "$library_path" "$cli_path" 2>/dev/null || true
  if command -v install_name_tool >/dev/null 2>&1 && command -v otool >/dev/null 2>&1; then
    old_id="$(otool -D "$library_path" 2>/dev/null | sed -n '2p')"
    if [[ -n "$old_id" && "$old_id" != "$library_path" ]]; then
      install_name_tool -id "$library_path" "$library_path"
    fi
    while IFS= read -r dependency; do
      [[ -n "$dependency" && "$dependency" != "$library_path" ]] || continue
      install_name_tool -change "$dependency" "$library_path" "$cli_path"
    done < <(otool -L "$cli_path" | awk 'NR > 1 {print $1}' | grep '/libfdb_c[^/]*\.dylib$' || true)
  fi
fi

if [[ -z "$library_path" || ! -f "$library_path" ]]; then
  echo "FoundationDB client library was not found after extraction" >&2
  exit 1
fi
if [[ -z "$cli_path" || ! -f "$cli_path" ]]; then
  echo "fdbcli was not found after extraction" >&2
  exit 1
fi
chmod +x "$cli_path"
if ! LC_ALL=C grep -a -q "$VERSION" "$library_path"; then
  echo "FoundationDB client library does not identify itself as version $VERSION" >&2
  exit 1
fi

write_manifest "$manifest" "$library_path" "$cli_path"

echo "Prepared FoundationDB client $VERSION for $platform"
echo "  asset SHA-256: $actual_sha256"
echo "  library: $library_path"
echo "  fdbcli: $cli_path"
