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
"""Default Cargo compilation to half the host's logical CPUs."""
import os
import sys


def configure(environment=None):
    environment = os.environ if environment is None else environment
    if "CARGO_BUILD_JOBS" not in environment:
        cpus = os.cpu_count()
        if cpus is None or cpus < 1:
            raise RuntimeError("cannot determine the logical CPU count for Cargo build jobs")
        environment["CARGO_BUILD_JOBS"] = str(max(1, cpus // 2))
    return environment["CARGO_BUILD_JOBS"]


if __name__ == "__main__":
    try:
        print(configure())
    except RuntimeError as error:
        print("error: " + str(error), file=sys.stderr)
        sys.exit(2)
