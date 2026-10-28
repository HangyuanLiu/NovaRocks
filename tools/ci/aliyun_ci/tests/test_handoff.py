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

"""Original bytes and independent checksums protect disposable-worker handoff.

These local functional tests belong to CI maintainers and run on handoff or
workflow changes. HTTP servers own shutdown/join; no cloud resources exist.
"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
from pathlib import Path
import tarfile
import tempfile
import threading
import unittest

from aliyun_ci import handoff


class HandoffTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.attempt = self.root / 'attempt'
        self.attempt.mkdir()
        (self.attempt / 'stdout.log').write_bytes(b'original Rust failure\n')
        (self.attempt / 'nested').mkdir()
        (self.attempt / 'nested/index.json').write_text('{"exit_code":100}')

    def archive(self, kind='final'):
        return handoff.pack(self.attempt, kind=kind, output=self.root / f'{kind}.tar.gz')

    def changed_archive(self, data, *, omit=None, replace=None):
        source = tarfile.open(fileobj=io.BytesIO(data), mode='r:gz')
        target = io.BytesIO()
        with source, tarfile.open(fileobj=target, mode='w:gz') as out:
            for item in source:
                if item.name == omit:
                    continue
                content = source.extractfile(item).read()
                if replace and item.name == replace:
                    content = b'x' * len(content)
                item.size = len(content)
                out.addfile(item, io.BytesIO(content))
        return target.getvalue()

    def test_original_members_are_complete_and_checked_without_extracting(self):
        archive = self.archive()
        fact = handoff.verify(archive)
        self.assertTrue(fact['manifest_ok'], fact)
        self.assertEqual('final', fact['kind'])
        self.assertTrue(fact['complete'])
        self.assertEqual(fact, handoff.verify(archive.read_bytes()))
        with tarfile.open(archive, 'r:gz') as source:
            self.assertEqual(b'original Rust failure\n', source.extractfile('attempt/stdout.log').read())

    def test_truncated_archive_missing_member_and_wrong_digest_are_different_gaps(self):
        data = self.archive().read_bytes()
        truncated = handoff.verify(data[:-8])
        self.assertFalse(truncated['manifest_ok'])
        missing = handoff.verify(self.changed_archive(data, omit='attempt/stdout.log'))
        self.assertEqual(['attempt/stdout.log'], missing['missing_members'])
        wrong = handoff.verify(self.changed_archive(data, replace='attempt/stdout.log'))
        self.assertFalse(wrong['manifest_ok'])
        self.assertTrue(any('SHA-256 differs' in error for error in wrong['errors']))

    def test_interrupted_is_not_final_and_absent_is_explicit(self):
        fact = handoff.verify(self.archive('interrupted'))
        self.assertTrue(fact['manifest_ok'])
        self.assertFalse(fact['complete'])
        self.assertEqual('interrupted', fact['kind'])
        self.assertFalse(handoff.verify(self.root / 'absent')['present'])

    def test_pack_rejects_links_and_output_in_attempt(self):
        (self.attempt / 'leak').symlink_to('/etc/passwd')
        with self.assertRaisesRegex(ValueError, 'symbolic link'):
            self.archive()
        (self.attempt / 'leak').unlink()
        with self.assertRaisesRegex(ValueError, 'outside'):
            handoff.pack(self.attempt, output=self.attempt / 'archive.tar.gz')

    def test_upload_receives_exact_archive_bytes(self):
        received = []
        class Handler(BaseHTTPRequestHandler):
            def do_PUT(self):
                received.append((self.path, self.headers.get('Content-Type'),
                                 self.rfile.read(int(self.headers['Content-Length']))))
                self.send_response(200)
                self.end_headers()
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            result = handoff.pack_upload(self.attempt, kind='final',
                output=self.root / 'uploaded.tar.gz', url=f'http://127.0.0.1:{server.server_port}/exact-key')
            self.assertTrue(result['uploaded'])
            self.assertEqual('/exact-key', received[0][0])
            self.assertEqual('', received[0][1])
            self.assertEqual(Path(result['archive']).read_bytes(), received[0][2])
            self.assertTrue(handoff.verify(received[0][2])['manifest_ok'])
        finally:
            server.shutdown()
            server.server_close()
            thread.join()
