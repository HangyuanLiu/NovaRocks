#!/usr/bin/env python3
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

"""Plan or collect runnable static/dynamic/residual cells; never infer full matrix acceptance."""
import argparse
import hashlib
import itertools
import json
import os
from pathlib import Path
import platform
import subprocess
import time

FROZEN_HASH = "cd3f06768bd6b757860b8c17c724a31469eb880ab2674cd608d7b13693a0d676"
MODES = ("same-thread", "round-robin", "fan-in")
CASES = ("funded", "refill", "debt", "refusal", "oscillation", "alternating-sponsor")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--historical-binary", type=Path)
    parser.add_argument("--historical-receipt", type=Path)
    parser.add_argument("--manifest", type=Path, default=Path(__file__).with_name("stock_cost_manifest.json"))
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--available-cpus", required=True, type=int, help="Frozen N")
    parser.add_argument("--affinity", help="Linux taskset CPU list; identical for both variants")
    parser.add_argument("--machine-receipt", type=Path)
    parser.add_argument("--calibration-receipt", type=Path)
    parser.add_argument("--execute", action="store_true", help="Otherwise only persist the complete job plan")
    parser.add_argument("--smoke", action="store_true", help="Small entrance check; never formal acceptance")
    parser.add_argument("--family", choices=("all", "static", "dynamic", "residual"), default="all")
    parser.add_argument("--timeout-secs", type=int, default=3600)
    parser.add_argument("--case", choices=CASES, action="append", help="Select a subset and mark it incomplete")
    args = parser.parse_args()
    content = args.manifest.read_bytes()
    digest = hashlib.sha256(content).hexdigest()
    if digest != FROZEN_HASH:
        parser.error(f"manifest hash mismatch: {digest}")
    manifest = json.loads(content)
    if args.available_cpus < 1:
        parser.error("available CPUs must be positive")
    if args.execute and not args.smoke:
        if platform.system() != "Linux" or not args.affinity or not args.machine_receipt or not args.calibration_receipt:
            parser.error("formal collection needs native Linux, affinity, machine and once-only calibration receipts")
        if not args.machine_receipt.is_file() or not args.calibration_receipt.is_file():
            parser.error("receipt files must exist before collection")
    calibration = json.loads(args.calibration_receipt.read_text()) if args.calibration_receipt else None
    if calibration and (calibration.get("kind") != "once_only_supply_calibration" or calibration.get("provenance",{}).get("manifest_sha256") != FROZEN_HASH):
        parser.error("calibration receipt kind/hash does not match frozen input")
    historical = json.loads(args.historical_receipt.read_text()) if args.historical_receipt else None
    if bool(args.historical_binary) != bool(historical):
        parser.error("historical binary and preparation receipt must be supplied together")
    if historical:
        template_hash = hashlib.sha256(Path(__file__).with_name("historical_stock_cost.rs.in").read_bytes()).hexdigest()
        if historical.get("kind") != "external_historical_harness_preparation" or historical.get("historical_sha") != manifest["historical_reservation_baseline_sha"] or historical.get("manifest_sha256") != FROZEN_HASH or historical.get("template_sha256") != template_hash or historical.get("baseline_core_dirty") is not False:
            parser.error("historical preparation receipt/source/template differs from frozen comparator")
        package = Path(historical["external_package"])
        if historical.get("build_status") != "passed" or hashlib.sha256((package / "src/main.rs").read_bytes()).hexdigest() != historical.get("materialized_adapter_sha256"):
            parser.error("historical adapter source/build receipt changed or build did not pass")
        if not args.historical_binary.is_file():
            parser.error("historical binary must exist")
    if args.execute and not args.smoke and args.family in ("all", "static") and not historical:
        parser.error("formal static collection requires the external historical binary and preparation receipt")
    if args.output.exists() and any(args.output.iterdir()):
        parser.error("result output must be empty or absent; existing evidence will not be overwritten")
    args.output.mkdir(parents=True, exist_ok=True)
    binary = str(args.binary.resolve())
    manifest_path = str(args.manifest.resolve())
    n = args.available_cpus
    thread_counts = sorted({1, min(8, n), n, 2 * n})
    cases = tuple(args.case or CASES)
    if args.smoke:
        axes = ([min(2, n)], [4096], [0], [16], [262144], [2], [2], [2])
        rounds, pairs = 1, 100
    else:
        axes = (thread_counts, manifest["matrix"]["operation_bytes"], [0, 1, 5, 20],
                manifest["matrix"]["scope_switch_operations"],
                manifest["protocol"]["quantum_sensitivity_bytes"],
                manifest["matrix"]["accounts"], manifest["matrix"]["tree_depths"], [1, 4, 16])
        rounds = manifest["sampling"]["independent_process_rounds_minimum"]
        pairs = manifest["sampling"]["successful_pairs_per_success_cell_minimum"]
    index = {"manifest_sha256": digest, "matrix_complete": False, "formal_acceptance": False,
             "smoke": args.smoke, "available_cpus_N": n, "affinity": args.affinity,
             "machine_receipt": str(args.machine_receipt) if args.machine_receipt else None,
             "calibration_receipt": str(args.calibration_receipt) if args.calibration_receipt else None,
             "historical_preparation_receipt": str(args.historical_receipt) if args.historical_receipt else None,
             "selected_case_subset": list(args.case) if args.case else None, "selected_family": args.family,
             "independent_processes_per_cell_variant": rounds, "planned_jobs": 0,
             "completed_jobs": 0, "failed_jobs": 0,
             "relative_gate_limits": {"scope_churn_refill": "historical public API has no equivalent partial-return operation; native-policy diagnostics excluded from relative pass evaluation; absolute service/progress gates apply"},
             "missing_families": ["historical_reservation_external_results",

                                  "hardware_growth_gate_cacheline_atomic_cost"]}
    if historical:
        index["missing_families"].remove("historical_reservation_external_results")
    index_path = args.output / "index.json"
    # Streaming enumeration avoids retaining a potentially large Cartesian
    # plan in memory. No unsupported cell is silently marked as executed.
    with (args.output / "jobs.jsonl").open("w") as jobs:
        static_cases = cases if args.family in ("all", "static") else ()
        for cell, case, mode in itertools.product(itertools.product(*axes), static_cases, MODES):
            t, size, work, interval, quantum, accounts, depth, domains = cell
            if case == "alternating-sponsor" and (accounts < 2 or domains < 2):
                jobs.write(json.dumps({"status": "not_applicable", "reason": "requires two sponsors and lanes", "case": case, "cell": cell, "mode": mode}) + "\n")
                continue
            for round_number in range(rounds):
                order = ("baseline", "historical", "stock") if historical and case in ("funded", "refill", "alternating-sponsor") else ("baseline", "stock")
                if round_number % 2:
                    order = tuple(reversed(order))
                equal_work_signature = None
                for variant in order:
                    job_id = index["planned_jobs"]
                    index["planned_jobs"] += 1
                    command = [str(args.historical_binary.resolve()) if variant == "historical" else binary, "--manifest", manifest_path, "--threads", str(t), "--pairs", str(pairs),
                               "--rounds", "1", "--bytes", str(size), "--work-us", str(work),
                               "--scope-interval", str(interval), "--quantum", str(quantum),
                               "--accounts", str(accounts), "--depth", str(depth),
                               "--domains-per-thread", str(domains), "--case", case,
                               "--mode", mode, "--variant", variant]
                    if calibration:
                        command += ["--work-iterations", str(calibration["iterations"][str(work)])]
                    if args.affinity:
                        command = ["taskset", "-c", args.affinity] + command
                    job = {"job": job_id, "case": case, "round": round_number, "variant": variant,
                           "command": command, "status": "planned", "matrix_complete": False,
                           "semantic_equivalence": variant != "historical" or case != "refill",
                           "relative_cost_gate_applicable": not (variant == "historical" and case == "refill")}
                    if args.execute:
                        directory = args.output / f"job-{job_id:08d}"
                        directory.mkdir(exist_ok=False)
                        env = os.environ.copy()
                        env["STOCK_COST_SAVE_SAMPLES"] = str((directory / "samples").resolve())
                        started = time.monotonic_ns()
                        with (directory / "stdout.jsonl").open("w") as out, (directory / "stderr.log").open("w") as err:
                            try:
                                result = subprocess.run(command, stdout=out, stderr=err, env=env, check=False, timeout=args.timeout_secs)
                            except subprocess.TimeoutExpired:
                                err.write("Independent process exceeded timeout; partial evidence retained.\n")
                                result = subprocess.CompletedProcess(command, 124)
                        if result.returncode == 0:
                            try:
                                records = [json.loads(line) for line in (directory / "stdout.jsonl").read_text().splitlines() if line.strip()]
                                if len(records) != 1:
                                    raise ValueError("expected one independent round record")
                                record = records[0]
                                signature = tuple(record[key] for key in ("successful_pairs", "free_acknowledged_bytes", "checksum"))
                                if signature[:2] != (t*pairs, t*pairs*size):
                                    raise ValueError("iteration/free-byte conservation mismatch")
                                if equal_work_signature is not None and signature != equal_work_signature:
                                    raise ValueError("comparator equal-work checksum/freebytes mismatch")
                                equal_work_signature = signature
                                if variant == "historical" and (record.get("source_sha") != manifest["historical_reservation_baseline_sha"] or record.get("adapter_source_sha256") != historical["template_sha256"]):
                                    raise ValueError("historical runtime source provenance mismatch")
                            except (ValueError, KeyError) as error:
                                result = subprocess.CompletedProcess(command, 2)
                                job["verification_error"] = str(error)
                        job.update(status="completed" if result.returncode == 0 else "failed",
                                   exit_code=result.returncode, elapsed_ns=time.monotonic_ns()-started,
                                   evidence_directory=str(directory.resolve()))
                        index["completed_jobs" if result.returncode == 0 else "failed_jobs"] += 1
                    jobs.write(json.dumps(job) + "\n")
                    jobs.flush()
                    index_path.write_text(json.dumps(index, indent=2) + "\n")
    if args.family in ("all", "dynamic", "residual"):
        if args.smoke:
            dynamic_axes = ([min(2,n)], [5], [4096], [16], [262144], [2], [2], [2], [0,128], ["before","during","after"])
        else:
            dynamic_axes = ([t for t in thread_counts if t<=n], [5,20], manifest["matrix"]["operation_bytes"], [1,16,256],
                            manifest["protocol"]["quantum_sensitivity_bytes"], [1,16,64], [2,4,8], [1,4,16],
                            manifest["matrix"]["historical_residual_counts"], ["before","during","after"])
        if args.family == "residual":
            dynamic_axes = ([1], [5] if args.smoke else [5,20], [4096] if args.smoke else manifest["matrix"]["operation_bytes"], [16], [262144], [1], [2],
                            manifest["matrix"]["active_owner_counts"],
                            [128] if args.smoke else manifest["matrix"]["historical_residual_counts"],
                            ["before","during","after"])
        cells = itertools.product(*dynamic_axes)
        if args.family == "all":
            residual_axes = ([1], [5] if args.smoke else [5,20],
                             [4096] if args.smoke else manifest["matrix"]["operation_bytes"],
                             [16], [262144], [1], [2], manifest["matrix"]["active_owner_counts"],
                             [128] if args.smoke else manifest["matrix"]["historical_residual_counts"],
                             ["before","during","after"])
            cells = itertools.chain(cells,itertools.product(*residual_axes))
        with (args.output / "jobs.jsonl").open("a") as jobs:
            for cell, mode in ((cell, mode) for cell in cells for mode in MODES):
                t,size_work,size,interval,quantum,accounts,depth,domains,residuals,phase=cell
                for round_number in range(rounds):
                    job_id=index["planned_jobs"];index["planned_jobs"]+=1
                    command=[binary,"--manifest",manifest_path,"--service","dynamic","--threads",str(t),
                             "--rounds","1","--bytes",str(size),"--work-us",str(size_work),
                             "--scope-interval",str(interval),"--quantum",str(quantum),"--accounts",str(accounts),
                             "--depth",str(depth),"--domains-per-thread",str(domains),"--mode",mode,
                             "--historical-residuals",str(residuals),"--late-free-phase",phase,
                             "--duration-ms","60" if args.smoke else "30000"]
                    if calibration:command += ["--work-iterations",str(calibration["iterations"][str(size_work)])]
                    if args.affinity:command=["taskset","-c",args.affinity]+command
                    job={"job":job_id,"family":"dynamic" if args.family!="residual" else "residual",
                         "cell":cell,"round":round_number,"command":command,"status":"planned","matrix_complete":False}
                    if args.execute:
                        directory=args.output/f"job-{job_id:08d}";directory.mkdir(exist_ok=False)
                        started=time.monotonic_ns()
                        with (directory/"stdout.jsonl").open("w") as out,(directory/"stderr.log").open("w") as err:
                            try:
                                result=subprocess.run(command,stdout=out,stderr=err,check=False,timeout=args.timeout_secs)
                            except subprocess.TimeoutExpired:
                                err.write("Independent process exceeded timeout; partial evidence retained.\n")
                                result=subprocess.CompletedProcess(command,124)
                        job.update(status="completed" if result.returncode==0 else "failed",exit_code=result.returncode,
                                   elapsed_ns=time.monotonic_ns()-started,evidence_directory=str(directory.resolve()))
                        index["completed_jobs" if result.returncode==0 else "failed_jobs"]+=1
                    jobs.write(json.dumps(job)+"\n");jobs.flush();index_path.write_text(json.dumps(index,indent=2)+"\n")
    index_path.write_text(json.dumps(index, indent=2) + "\n")
    print(json.dumps(index))
    return 1 if index["failed_jobs"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
