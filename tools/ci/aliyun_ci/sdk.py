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

"""Lazy official SDK adapters. All callers use the same standard credential chain."""
from __future__ import annotations
from importlib import import_module
import json
import os
from .ports import APIError
from .plan import parse_time


def rpc_parameters(parameters):
    """Alibaba RPC repeated structures use the documented one-based dotted keys."""
    result = {}
    def visit(key, value):
        if isinstance(value, dict):
            for child, item in value.items():
                visit(f'{key}.{child}', item)
        elif isinstance(value, list):
            for index, item in enumerate(value, 1):
                visit(f'{key}.{index}', item)
        elif value is not None:
            result[key] = str(value).lower() if isinstance(value, bool) else str(value)
    for key, value in parameters.items():
        visit(key, value)
    return result


class RPC:
    SERVICES = {
        'ecs': ('alibabacloud_ecs20140526', '2014-05-26', 'ecs'),
        'oos': ('alibabacloud_oos20190601', '2019-06-01', 'oos'),
        'ros': ('alibabacloud_ros20190910', '2019-09-10', 'ros'),
        'sts': ('alibabacloud_sts20150401', '2015-04-01', 'sts'),
    }

    def __init__(self, service, region, *, credential=None):
        package, self.version, _ = self.SERVICES[service]
        self.service, self.region = service, region
        self.models = import_module('alibabacloud_tea_openapi.models')
        self.runtime_models = import_module('alibabacloud_tea_util.models')
        credential = credential or import_module('alibabacloud_credentials.client').Client()
        # Endpoint mappings belong to the pinned official service client.
        config = self.models.Config(credential=credential, region_id=region)
        self.client = import_module(package + '.client').Client(config)

    def call(self, action, **parameters):
        # SDK transport retry is disabled: mutations recover with their original token.
        try:
            request = self.models.OpenApiRequest(query=rpc_parameters(parameters))
            params = self.models.Params(action=action, version=self.version, protocol='HTTPS',
                pathname='/', method='POST', auth_type='AK', style='RPC', req_body_type='formData', body_type='json')
            response = self.client.call_api(params, request, self.runtime_models.RuntimeOptions(autoretry=False))
            return response['body']
        except Exception as error:
            code = getattr(error, 'code', None)
            if code:
                raise APIError(code, f'{self.service}.{action} failed') from None
            if isinstance(error, (TimeoutError, ConnectionError)):
                raise
            raise APIError('TransportUnknown', type(error).__name__) from None


def check_account(config, sts):
    identity = sts.call('GetCallerIdentity')
    if str(identity.get('AccountId')) != str(config['account_id']):
        raise ValueError('credential account does not match deployment account_id')
    return identity


class ObjectStore:
    def __init__(self, config, *, credential=None):
        self.sdk = import_module('alibabacloud_oss_v2')
        credential = credential or import_module('alibabacloud_credentials.client').Client()
        self.credential = credential
        def get_credentials():
            value = credential.get_credential()
            return self.sdk.credentials.Credentials(value.access_key_id, value.access_key_secret,
                                                    value.security_token)
        settings = self.sdk.config.load_default()
        settings.region = config['region']
        settings.credentials_provider = self.sdk.credentials.CredentialsProviderFunc(get_credentials)
        self.client, self.bucket = self.sdk.Client(settings), config['oss']['bucket']

    def put(self, key, data, *, write_once=False):
        try:
            self.client.put_object(self.sdk.PutObjectRequest(bucket=self.bucket, key=key, body=data,
                                                            forbid_overwrite=write_once))
        except Exception as error:
            code = getattr(error, 'code', type(error).__name__)
            if write_once and code in ('FileAlreadyExists', 'ObjectAlreadyExists'):
                if self.get(key) == data:
                    return
                raise APIError('ImmutableObjectConflict', f'object already contains different bytes: {key}') from error
            raise APIError(code, 'OSS PUT failed') from error

    def get(self, key):
        try:
            result = self.client.get_object(self.sdk.GetObjectRequest(bucket=self.bucket, key=key))
            try:
                return result.body.read()
            finally:
                result.body.close()
        except Exception as error:
            raise APIError(getattr(error, 'code', type(error).__name__), 'OSS GET failed') from error

    def list(self, prefix):
        keys, token = [], None
        while True:
            value = self.client.list_objects_v2(self.sdk.ListObjectsV2Request(
                bucket=self.bucket, prefix=prefix, continuation_token=token))
            keys.extend(item.key for item in (value.contents or []))
            if not value.is_truncated:
                return keys
            token = value.next_continuation_token
            if not token:
                raise ValueError('truncated OSS list has no continuation token')

    def presign(self, method, key, expires_at):
        expiration = parse_time(expires_at)
        current = self.credential.get_credential()
        if current.security_token:
            bound = os.environ.get('ALIYUN_CI_STS_NOT_AFTER')
            if not bound:
                raise ValueError('temporary signing credentials require ALIYUN_CI_STS_NOT_AFTER')
            if expiration > parse_time(bound):
                raise ValueError('object capability outlives the known STS credential expiry')
        request_type = {'PUT': self.sdk.PutObjectRequest, 'GET': self.sdk.GetObjectRequest}.get(method)
        if request_type is None:
            raise ValueError('only GET and PUT object capabilities are supported')
        result = self.client.presign(request_type(bucket=self.bucket, key=key), expiration=expiration)
        if result.signed_headers:
            raise ValueError('bootstrap upload capability unexpectedly requires signed headers')
        return result.url


def adapters(config):
    credential = import_module('alibabacloud_credentials.client').Client()
    sts = RPC('sts', config['region'], credential=credential)
    check_account(config, sts)
    return {name: RPC(name, config['region'], credential=credential) for name in ('ecs', 'oos', 'ros')} | {
        'oss': ObjectStore(config, credential=credential)}
