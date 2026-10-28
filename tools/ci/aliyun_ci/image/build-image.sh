#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail
# Run only on the disposable Ubuntu builder, as root. No cloud credentials are inputs.
source_root=$(realpath "${1:?source directory is required}")
preparation_sha=${2:?preparation SHA is required}
[[ "$preparation_sha" =~ ^[0-9a-f]{40}$ ]] || { echo 'Invalid preparation SHA' >&2; exit 2; }
[ "$(id -u)" -eq 0 ] || { echo 'Image preparation requires root' >&2; exit 2; }
if ! id runner >/dev/null 2>&1; then
  useradd --create-home --home-dir /var/lib/novarocks-ci/runner --shell /bin/bash runner
fi
install -d -m 0755 /opt/novarocks-ci /opt/novarocks-ci/tools-source/tools
install -d -g runner -m 0750 /var/log/novarocks-ci
install -g runner -m 0640 /dev/null /var/log/novarocks-ci/bootstrap.log
cp -a "$source_root/tools/dev" /opt/novarocks-ci/tools-source/tools/
cp "$source_root/rust-toolchain.toml" /opt/novarocks-ci/tools-source/
"$source_root/tools/dev/linux-env.sh" prepare --user runner \
  --profile "$source_root/tools/dev/profiles/aliyun-ubuntu26-amd64-tools.json" --tools-only
apt-get install --yes python3-venv iptables libicu-dev libkrb5-3 liblttng-ust-dev zlib1g
python3 -m venv /opt/novarocks-ci/venv
/opt/novarocks-ci/venv/bin/pip install --require-hashes -r "$source_root/tools/ci/aliyun_ci/requirements.lock"
cp -a "$source_root/tools/ci/aliyun_ci" /opt/novarocks-ci/
find /opt/novarocks-ci -type d -name __pycache__ -exec rm -rf {} +
install -m 0644 "$source_root"/tools/ci/aliyun_ci/instance/systemd/*.service /etc/systemd/system/
printf '%s\n' "$preparation_sha" > /opt/novarocks-ci/preparation-sha
chown -R root:root /opt/novarocks-ci
chmod -R go-w /opt/novarocks-ci
systemctl daemon-reload
systemctl enable novarocks-ci-bootstrap.service
"$source_root/tools/ci/aliyun_ci/image/harden.sh"
/opt/novarocks-ci/tools-source/tools/dev/linux-env.sh check --user runner \
  --profile /opt/novarocks-ci/tools-source/tools/dev/profiles/aliyun-ubuntu26-amd64-tools.json --tools-only \
  > /opt/novarocks-ci/tools-check.json
python3 - "$source_root" "$preparation_sha" <<'PY_FACTS'
import hashlib, json, platform, subprocess, sys
from pathlib import Path
root=Path(sys.argv[1])
digest=lambda path:hashlib.sha256((root/path).read_bytes()).hexdigest()
facts={'schema':1,'preparation_sha':sys.argv[2], 'os':'ubuntu-26.04','arch':'amd64',
       'tools_profile_digest':digest('tools/dev/profiles/aliyun-ubuntu26-amd64-tools.json'),
       'inputs_digest':digest('tools/dev/linux-env.inputs.json'),
       'platform_lock_digest':digest('tools/ci/aliyun_ci/requirements.lock'),
       'observed':{'python':platform.python_version(),'kernel':platform.release(),
          'runner_native_packages':subprocess.check_output(['dpkg-query','-W','-f=${Package}=${Version}\\n',
            'libicu-dev','libkrb5-3','liblttng-ust-dev','zlib1g'],text=True)},
       'tools_check':json.loads(Path('/opt/novarocks-ci/tools-check.json').read_text())}
Path('/opt/novarocks-ci/preparation-facts.json').write_text(json.dumps(facts,sort_keys=True)+'\n')
PY_FACTS
# Remove build-only source and machine identity before the image is captured.
rm -rf -- "$source_root"
rm -rf /root/.cache/pip /root/.cargo /root/.rustup
cloud-init clean --logs --machine-id
rm -f /etc/ssh/ssh_host_*
