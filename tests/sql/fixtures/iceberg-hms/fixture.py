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

"""Spark-owned HMS data and complete, paginated refusal evidence.

Requires an already provisioned REST/HMS publication. No provisioning or
downloads occur here. Only this invocation's unique namespace is cleaned up.
"""

import argparse
import datetime
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import subprocess
import signal
import sys
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ET


SPARK_PROGRAM = r'''
import json
import sys
import time
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
action, ns, warehouse = sys.argv[1:]
root = warehouse.rstrip('/') + '/iru7/' + ns
prefix = 'hms_catalog.' + ns
if action == 'prepare':
    spark.sql("CREATE NAMESPACE " + prefix + " LOCATION '" + root + "'")
    for version in [1, 2, 3]:
        table = prefix + '.v' + str(version)
        partitioning = ' PARTITIONED BY (p)' if version != 2 else ''
        lineage = ", 'write.row-lineage'='true'" if version == 3 else ''
        spark.sql("CREATE TABLE " + table + " (id BIGINT, p INT, note STRING) USING iceberg" + partitioning + " LOCATION '" + root + '/v' + str(version) + "' TBLPROPERTIES ('format-version'='" + str(version) + "', 'write.delete.mode'='merge-on-read'" + lineage + " )")
        # The unpartitioned v2 table needs all rows in one file so its partial
        # delete produces a position-delete file rather than removing a file.
        if version == 2:
            spark.sql("INSERT INTO " + table + " SELECT /*+ COALESCE(1) */ * FROM VALUES (1L, 1, 'one'), (2L, 2, 'two'), (3L, 1, 'three') AS data(id, p, note)")
        else:
            spark.sql("INSERT INTO " + table + " VALUES (1, 1, 'one'), (2, 2, 'two'), (3, 1, 'three')")
        if version > 1:
            spark.sql('DELETE FROM ' + table + ' WHERE id = 1')
            formats = {row.file_format for row in spark.sql('SELECT file_format FROM ' + table + '.files WHERE content = 1').collect()}
            expected = 'PARQUET' if version == 2 else 'PUFFIN'
            if formats != {expected}:
                raise RuntimeError('fixture lacks the required delete artifact: ' + table + ' ' + str(formats))
    spark.sql("ALTER TABLE " + prefix + ".v2 SET TBLPROPERTIES ('write.delete.mode'='copy-on-write')")
    spark.sql('ALTER TABLE ' + prefix + '.v2 CREATE BRANCH kept_branch')
    spark.sql('ALTER TABLE ' + prefix + '.v2 CREATE TAG kept_tag')
    locations = {}
    for version in [1, 2, 3]:
        actual = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, prefix + '.v' + str(version)).location()
        if actual != root + '/v' + str(version):
            raise RuntimeError('actual HMS location differs from this run scratch root: ' + actual)
        locations['v' + str(version)] = actual
    result = {'prepared': ns, 'actual_locations': locations}
elif action == 'snapshot':
    namespaces = sorted(row[0] for row in spark.sql('SHOW NAMESPACES IN hms_catalog').collect() if row[0] in [ns, ns + '_new'])
    result = {'namespaces': namespaces, 'tables': {}, 'views': {}, 'metadata_location': {}}
    for namespace in namespaces:
        qualified = 'hms_catalog.' + namespace
        names = sorted(row.tableName for row in spark.sql('SHOW TABLES IN ' + qualified).collect())
        result['tables'][namespace] = names
        try:
            result['views'][namespace] = sorted(row.viewName for row in spark.sql('SHOW VIEWS IN ' + qualified).collect())
        except Exception as error:
            if 'UnsupportedOperationException' not in str(error):
                raise
            result['views'][namespace] = 'unsupported by the Spark HMS catalog'
        for name in names:
            table = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, qualified + '.' + name)
            table.refresh()
            # This is the current pointer, not the last historical log entry.
            result['metadata_location'][namespace + '.' + name] = table.operations().current().metadataFileLocation()
elif action == 'cleanup':
    namespaces = [row[0] for row in spark.sql('SHOW NAMESPACES IN hms_catalog').collect() if row[0] in [ns, ns + '_new']]
    for namespace in namespaces:
        qualified = 'hms_catalog.' + namespace
        for row in spark.sql('SHOW TABLES IN ' + qualified).collect():
            spark.sql('DROP TABLE ' + qualified + '.' + row.tableName + ' PURGE')
        spark.sql('DROP NAMESPACE ' + qualified)
    result = {'cleaned': ns}
else:
    raise ValueError(action)
print('IRU7_FACTS=' + json.dumps(result, sort_keys=True))
spark.stop()
'''


def run_spark(workspace, action, namespace):
    required = ['NOVA_ENV_COMPOSE_ENV', 'NOVA_ENV_COMPOSE_PROJECT',
                'NOVA_ENV_COMPOSE_FILE', 'NOVAROCKS_SPARK_DEFAULTS',
                'NOVAROCKS_SPARK_HMS_DEFAULTS', 'NOVAROCKS_ICEBERG_HMS_WAREHOUSE']
    for key in required:
        if not os.environ.get(key):
            raise RuntimeError('source the exact REST and HMS publications: missing ' + key)
    compose = ['python3', str(workspace / 'docker/iceberg-rest/fixture_run.py'),
               'compose', '--env-file', os.environ['NOVA_ENV_REST_ENV_FILE'], '--',
               '--env-file', os.environ['NOVA_ENV_COMPOSE_ENV'],
               '-p', os.environ['NOVA_ENV_COMPOSE_PROJECT'],
               '-f', os.environ['NOVA_ENV_COMPOSE_FILE'], 'exec', '-T', 'spark']
    helper = [sys.executable, str(workspace / 'docker/iceberg-rest/fixture_run.py')]
    folder = subprocess.check_output([*helper, 'temp-dir', '--env-file', os.environ['NOVA_ENV_REST_ENV_FILE']], text=True).strip()
    defaults = '\n'.join(Path(os.environ[key]).read_text() for key in
                         ['NOVAROCKS_SPARK_DEFAULTS', 'NOVAROCKS_SPARK_HMS_DEFAULTS'])
    defaults += "\nspark.sql.catalog.hms_catalog.warehouse " + os.environ['NOVAROCKS_ICEBERG_HMS_WAREHOUSE'] + "\n"
    subprocess.run(compose + ['/bin/mkdir', '-p', folder], check=True)
    try:
        for name, content in [('fixture.py', SPARK_PROGRAM), ('spark-defaults.conf', defaults)]:
            subprocess.run(compose + ['/bin/bash', '-c', 'cat > ' + folder + '/' + name],
                           input=content, text=True, check=True)
        output = subprocess.run(compose + ['/opt/spark/bin/spark-submit',
                                 '--properties-file', folder + '/spark-defaults.conf',
                                 folder + '/fixture.py', action, namespace,
                                 os.environ['NOVAROCKS_ICEBERG_HMS_WAREHOUSE']],
                                text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if output.returncode:
            raise RuntimeError('Spark fixture failed:\n' + output.stdout + output.stderr)
        facts = [line.removeprefix('IRU7_FACTS=') for line in output.stdout.splitlines()
                 if line.startswith('IRU7_FACTS=')]
        if len(facts) != 1:
            raise RuntimeError('Spark did not publish exactly one facts record')
        return json.loads(facts[0])
    finally:
        alarm = signal.setitimer(signal.ITIMER_REAL, 0)
        cleanup_started = time.monotonic()
        try:
            subprocess.run([*helper, 'cleanup-dir', '--env-file', os.environ['NOVA_ENV_REST_ENV_FILE'], '--path', folder], check=True)
        finally:
            if alarm[0]:
                signal.setitimer(signal.ITIMER_REAL, max(0.001, alarm[0] - (time.monotonic() - cleanup_started)), alarm[1])


def s3_request(bucket, parameters, method="GET", key=""):
    """Sign a path-style ListObjectsV2 request using the saved publication."""
    endpoint = os.environ['AWS_S3_ENDPOINT'].rstrip('/')
    now = datetime.datetime.now(datetime.timezone.utc)
    stamp, day = now.strftime('%Y%m%dT%H%M%SZ'), now.strftime('%Y%m%d')
    region = os.environ.get('AWS_REGION', 'us-east-1')
    parsed = urllib.parse.urlsplit(endpoint)
    path = parsed.path + '/' + urllib.parse.quote(bucket, safe='')
    if key:
        path += '/' + urllib.parse.quote(key, safe='/')
    query = '&'.join(urllib.parse.quote(str(k), safe='-_.~') + '=' +
                     urllib.parse.quote(str(v), safe='-_.~') for k, v in sorted(parameters.items()))
    body_hash = hashlib.sha256(b'').hexdigest()
    headers = {'host': parsed.netloc, 'x-amz-content-sha256': body_hash, 'x-amz-date': stamp}
    token = os.environ.get('AWS_SESSION_TOKEN')
    if token:
        headers['x-amz-security-token'] = token
    canonical_headers = ''.join(k + ':' + v.strip() + '\n' for k, v in sorted(headers.items()))
    signed_headers = ';'.join(sorted(headers))
    canonical = '\n'.join([method, path, query, canonical_headers, signed_headers, body_hash])
    scope = day + '/' + region + '/s3/aws4_request'
    signing_key = ('AWS4' + os.environ['AWS_S3_SECRET_ACCESS_KEY']).encode()
    for component in [day, region, 's3', 'aws4_request']:
        signing_key = hmac.new(signing_key, component.encode(), hashlib.sha256).digest()
    string_to_sign = 'AWS4-HMAC-SHA256\n' + stamp + '\n' + scope + '\n' + hashlib.sha256(canonical.encode()).hexdigest()
    signature = hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()
    headers['Authorization'] = ('AWS4-HMAC-SHA256 Credential=' + os.environ['AWS_S3_ACCESS_KEY_ID'] +
                                '/' + scope + ', SignedHeaders=' + signed_headers + ', Signature=' + signature)
    request = urllib.request.Request(urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, path, query, '')),
                                     headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=30) as response:
        body = response.read()
        return ET.fromstring(body) if body else None


def inventory(namespace):
    warehouse = urllib.parse.urlsplit(os.environ['NOVAROCKS_ICEBERG_HMS_WAREHOUSE'])
    if warehouse.scheme != 's3' or not warehouse.netloc:
        raise RuntimeError('HMS fixture must use the generated s3 warehouse')
    base = warehouse.path.strip('/')
    # Include the explicit namespace location and the owner's default locations
    # for attempted sibling namespace/table creations. Never inventory only v3.
    prefixes = [base + '/iru7/' + namespace + '/', base + '/' + namespace + '/',
                base + '/' + namespace + '_new/', base + '/' + namespace + '.db/',
                base + '/' + namespace + '_new.db/']
    prefixes = [(warehouse.netloc, prefix) for prefix in prefixes]
    original = os.environ.get('NOVA_FIXTURE_HMS_ORIGINAL_WAREHOUSE')
    if original:
        original = urllib.parse.urlsplit(original)
        original_base = original.path.strip('/')
        prefixes += [(original.netloc, original_base + '/' + namespace + suffix) for suffix in
                     ['/', '_new/', '.db/', '_new.db/']]
    objects = {}
    pages = 0
    xml_ns = {'s': 'http://s3.amazonaws.com/doc/2006-03-01/'}
    for bucket, prefix in dict.fromkeys(prefixes):
        continuation = None
        seen_tokens = set()
        while True:
            parameters = {'list-type': '2', 'prefix': prefix, 'max-keys': '2'}
            if continuation is not None:
                parameters['continuation-token'] = continuation
            root = s3_request(bucket, parameters)
            pages += 1
            for node in root.findall('s:Contents', xml_ns):
                key = node.findtext('s:Key', namespaces=xml_ns)
                object_uri = 's3://' + bucket + '/' + (key or '')
                if not key or not key.startswith(prefix) or object_uri in objects:
                    raise RuntimeError('invalid or duplicate key in paginated inventory')
                etag = node.findtext('s:ETag', namespaces=xml_ns)
                size = node.findtext('s:Size', namespaces=xml_ns)
                if not etag or size is None:
                    raise RuntimeError('S3 inventory omitted etag or size')
                objects[object_uri] = {'etag': etag, 'size': int(size)}
            if root.findtext('s:IsTruncated', namespaces=xml_ns) == 'false':
                break
            continuation = root.findtext('s:NextContinuationToken', namespaces=xml_ns)
            if not continuation or continuation in seen_tokens:
                raise RuntimeError('missing or repeated S3 continuation token')
            seen_tokens.add(continuation)
    return objects, pages


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['prepare', 'before', 'after', 'cleanup'])
    parser.add_argument('namespace')
    parser.add_argument('--fixture-budget-secs', type=int, default=3600)
    arguments = parser.parse_args()
    os.environ["NOVA_FIXTURE_BUDGET_SECS"] = str(arguments.fixture_budget_secs)
    if not re.fullmatch(r'(?:iru7_[a-z0-9_]+|nr_scratch_[0-9]{13}_[0-9a-f]{32}(?:_[a-z0-9_]+)?)', arguments.namespace):
        parser.error('namespace must be an invocation-owned iru7_ identifier')
    workspace = Path(os.environ.get('NOVAROCKS_WORKSPACE_ROOT', Path(__file__).resolve().parents[4]))
    sys.path.insert(0, str(workspace / 'docker/iceberg-rest'))
    import fixture_run
    from scratch import resource_run
    env_file = Path(os.environ['NOVA_ENV_REST_ENV_FILE'])
    if arguments.action == "cleanup" and os.environ.get("NOVA_FIXTURE_RUN_CONTEXT") not in (None, "null"):
        os.environ["NOVA_FIXTURE_CLEANUP_MODE"] = "true"
    _, _, budget = fixture_run.admit(env_file)
    if budget:
        os.environ['NOVA_FIXTURE_RUN_CONTEXT'] = json.dumps(budget.as_dict(), sort_keys=True)
        def expired(number, frame):
            raise RuntimeError('original shared fixture deadline expired')
        signal.signal(signal.SIGALRM, expired)
        signal.setitimer(signal.ITIMER_REAL, max(0.001, budget.remaining_seconds(work=arguments.action != 'cleanup')))
        if arguments.action == 'prepare' and resource_run(arguments.namespace) != budget.identity.run_id:
            parser.error('shared HMS namespace must belong to the original scratch run')
        os.environ['NOVA_FIXTURE_HMS_ORIGINAL_WAREHOUSE'] = os.environ['NOVAROCKS_ICEBERG_HMS_WAREHOUSE']
        os.environ['NOVAROCKS_ICEBERG_HMS_WAREHOUSE'] = budget.identity.scope_root + '/hms'
    if arguments.action == 'prepare' and budget:
        fixture_run.startup_sweep(env_file, budget=budget)
    evidence = workspace / 'tests/sql/.runtime/hms-evidence' / arguments.namespace
    evidence.mkdir(parents=True, exist_ok=True)
    if arguments.action in ['before', 'after']:
        catalog = run_spark(workspace, 'snapshot', arguments.namespace)
        objects, pages = inventory(arguments.namespace)
        snapshot = {'catalog': catalog, 'objects': objects}
        (evidence / (arguments.action + '.json')).write_text(json.dumps(snapshot, indent=2, sort_keys=True) + '\n')
        if arguments.action == 'after':
            before = json.loads((evidence / 'before.json').read_text())
            if before != snapshot:
                raise RuntimeError('HMS catalog or object inventory changed; inspect ' + str(evidence))
            print('INVENTORY_UNCHANGED objects=' + str(len(objects)) + ' pages=' + str(pages))
        else:
            if len(catalog['metadata_location']) != 3 or len(objects) <= 2 or pages <= 5:
                raise RuntimeError('before snapshot lacks the complete, paginated three-table fixture')
            print('INVENTORY_CAPTURED objects=' + str(len(objects)) + ' pages=' + str(pages))
    else:
        run_spark(workspace, arguments.action, arguments.namespace)
        if arguments.action == 'cleanup':
            # Spark PURGE owns catalog teardown. Remove only remaining objects
            # in the exact random namespace prefixes, including directory markers.
            objects, _ = inventory(arguments.namespace)
            for uri in objects:
                object_location = urllib.parse.urlsplit(uri)
                s3_request(object_location.netloc, {}, method='DELETE', key=object_location.path.lstrip('/'))
            remaining, _ = inventory(arguments.namespace)
            if remaining:
                raise RuntimeError('private HMS fixture objects remain after cleanup')
        print('HMS_' + arguments.action.upper() + '_OK')


if __name__ == '__main__':
    main()
