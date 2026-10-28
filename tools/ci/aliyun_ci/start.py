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

"""One-shot spot allocation. Unknown create outcomes keep their original token."""
from __future__ import annotations
import base64
from datetime import datetime, timedelta, timezone
import hashlib
import json
import time
from .ports import APIError
from .plan import identity_tags, parse_time
from .github_api import generate_jit, runners, validate_target

NO_STOCK = {'OperationDenied.NoStock', 'Zone.NotOnSale'}
UNKNOWN = {'TransportUnknown', 'RequestTimeout', 'InternalError'}
NOT_FOUND = {'SecretParameterNotFound', 'InvalidSecretParameter.NotFound', 'ParameterNotFound'}


def tags_of(instance):
    return {item['TagKey']: item['TagValue'] for item in instance.get('Tags', {}).get('Tag', [])}


def tag_query(tags):
    return [{'Key': key, 'Value': value} for key, value in sorted(tags.items())]


def describe_instances(config, ecs, *, tags=None, instance_id=None):
    page, result = 1, []
    while True:
        params = {'RegionId': config['region'], 'PageSize': 100, 'PageNumber': page}
        if tags:
            params['Tag'] = tag_query(tags)
        if instance_id:
            params['InstanceIds'] = json.dumps([instance_id])
        response = ecs.call('DescribeInstances', **params)
        rows = response.get('Instances', {}).get('Instance', [])
        result.extend(rows)
        if len(result) >= response.get('TotalCount', len(result)):
            return result
        if not rows:
            raise ValueError('truncated ECS list returned no instances')
        page += 1


def owned(config, instance, expected_tags):
    actual = tags_of(instance)
    if instance.get('RegionId') != config['region'] or any(actual.get(k) != v for k, v in expected_tags.items()):
        raise ValueError(f"instance ownership mismatch: {instance.get('InstanceId')}")
    # The account is established by STS before constructing the service adapters.
    for field in ('deployment', 'repository', 'run_id', 'run_attempt', 'role', 'origin', 'deadline'):
        if field not in actual:
            raise ValueError(f'instance is missing ownership field {field}')
    parse_time(actual['deadline'])
    identity_tags(config, actual['run_id'], int(actual['run_attempt']), actual['role'])
    return actual


def find_run(config, plan, ecs):
    exact = {key: value for key, value in plan.tags.items() if key != 'deadline'}
    values = describe_instances(config, ecs, tags=exact)
    for instance in values:
        owned(config, instance, exact)
    if len(values) > 1:
        raise ValueError('multiple instances claim one run; refusing to choose one')
    return values[0] if values else None


def create_once(config, plan, ecs, image_id, *, now=None, user_data=None):
    now = now or (lambda: datetime.now(timezone.utc))
    existing = find_run(config, plan, ecs)
    if existing:
        return existing['InstanceId']
    if parse_time(plan.auto_release) < now() + timedelta(minutes=30):
        raise ValueError('auto release is less than 30 minutes from actual allocation')
    pointer = user_data or {'region': config['region'], 'secret_name': plan.secret_name,
                           'deadline': plan.deadline}
    for index, candidate in enumerate(config['candidates']):
        token = f'{plan.client_token}-c{index}'
        parameters = dict(RegionId=config['region'], ZoneId=candidate['zone'],
            VSwitchId=candidate['vswitch_id'], InstanceType=candidate['instance_type'],
            ImageId=image_id, SecurityGroupId=config['security_group_id'],
            RamRoleName=config['instance_ram_role'], InstanceChargeType='PostPaid',
            SpotStrategy='SpotAsPriceGo', SpotDuration=1, SpotInterruptionBehavior='Terminate',
            Amount=1, ClientToken=token, Tag=tag_query(plan.tags),
            AutoReleaseTime=plan.auto_release, SystemDisk={'Size': config['system_disk_gib'], 'Category': 'cloud_essd'},
            InternetMaxBandwidthOut=config['internet_max_bandwidth_out_mbps'], InternetChargeType='PayByTraffic',
            UserData=base64.b64encode(json.dumps(pointer, sort_keys=True).encode()).decode())
        modify = False
        for invocation in range(3):
            try:
                response = ecs.call('RunInstances', **parameters)
                ids = response.get('InstanceIdSets', {}).get('InstanceIdSet', [])
                if len(ids) != 1:
                    raise ValueError('RunInstances did not return exactly one instance')
                instance_id = ids[0]
                if modify:
                    ecs.call('ModifyInstanceAutoReleaseTime', RegionId=config['region'],
                             InstanceId=instance_id, AutoReleaseTime=plan.auto_release)
                return instance_id
            except APIError as error:
                if error.code in NO_STOCK:
                    break
                if error.code == 'InvalidAutoReleaseTime.Malformed' and not modify:
                    parameters.pop('AutoReleaseTime')
                    modify = True
                    continue
                if error.code not in UNKNOWN:
                    raise
            except (TimeoutError, ConnectionError):
                pass
            # Recover the same request; never move to a new candidate after an unknown result.
            if invocation == 0:
                continue
            existing = find_run(config, plan, ecs)
            if existing:
                instance_id = existing['InstanceId']
                if modify:
                    ecs.call('ModifyInstanceAutoReleaseTime', RegionId=config['region'],
                             InstanceId=instance_id, AutoReleaseTime=plan.auto_release)
                return instance_id
            raise APIError('CreateOutcomeUnknown', f'cannot resolve ClientToken {token}')
    raise APIError('NoCapacity', 'all configured spot candidates reported no stock')


def verify_instance(config, plan, instance, image_id):
    owned(config, instance, plan.tags)
    if instance.get('ImageId') != image_id or parse_time(instance.get('AutoReleaseTime', '')) != parse_time(plan.auto_release):
        raise ValueError(f"instance image or auto release mismatch: {instance['InstanceId']}")
    return instance


def wait_running(config, plan, ecs, instance_id, image_id, *, now=None, sleep=time.sleep):
    now = now or (lambda: datetime.now(timezone.utc))
    while now() < parse_time(plan.deadline):
        values = describe_instances(config, ecs, instance_id=instance_id)
        if values:
            instance = verify_instance(config, plan, values[0], image_id)
            if instance.get('Status') == 'Running':
                return instance
            if instance.get('Status') not in ('Pending', 'Starting', 'Stopped'):
                raise ValueError(f"unexpected instance status: {instance.get('Status')}")
        sleep(5)
    raise TimeoutError(f'instance {instance_id} did not reach Running before deadline')


def publish_bootstrap(config, plan, oos, payload):
    raw = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()
    encoded = base64.b64encode(raw).decode('ascii')
    parts = [encoded[i:i+4096] for i in range(0, len(encoded), 4096)]
    for index, part in enumerate(parts):
        oos.call('CreateSecretParameter', RegionId=config['region'],
                 Name=f'{plan.secret_name}-part-{index:06d}', Type='Secret', Value=part, Tags=json.dumps(plan.tags))
    root = {'schema': 1, 'parts': len(parts), 'sha256': hashlib.sha256(raw).hexdigest()}
    oos.call('CreateSecretParameter', RegionId=config['region'], Name=plan.secret_name,
             Type='Secret', Value=json.dumps(root, sort_keys=True), Tags=json.dumps(plan.tags))


# Design: ADR-0204 (docs/adr/ADR-0204-one-shot-spot-ci-and-independent-evidence.md)
def start_run(config, plan, ecs, oos, oss, github, *, sha, image_id, runner,
              mode='rust', now=None, sleep=time.sleep, wait_online=True):
    if mode not in ('rust', 'probe'):
        raise ValueError('unsupported run mode')
    validate_target(config, github, sha)
    release = json.loads(oss.get(f"{config['oss']['prefix']}/images/{image_id}/release.json"))
    if release.get('image_id') != image_id or release.get('region') != config['region']:
        raise ValueError('image release identity does not match request')
    upload_urls = {kind: oss.presign('PUT', plan.oss_keys[kind], plan.deadline) for kind in ('final', 'interrupted')}
    instance_id = create_once(config, plan, ecs, image_id, now=now)
    try:
        wait_running(config, plan, ecs, instance_id, image_id, now=now, sleep=sleep)
    except Exception:
        # Ownership is independently checked before cleanup; preserve the original mismatch.
        from .teardown import delete_exact
        delete_exact(config, ecs, instance_id, {key: value for key, value in plan.tags.items() if key != 'deadline'},
                     deadline=plan.deadline, now=now, sleep=sleep)
        raise
    jit = generate_jit(config, plan, github, instance_id=instance_id)
    payload = {'jit': jit, 'runner': runner, 'deadline': plan.deadline,
        'attempt_root': '/var/lib/novarocks-ci/attempt',
        'upload_urls': upload_urls,
        'watcher': config['watcher'], 'identity': {'run_id': plan.run_id, 'run_attempt': plan.run_attempt,
            'label': plan.label, 'image_id': image_id, 'instance_id': instance_id}}
    publish_bootstrap(config, plan, oos, payload)
    if wait_online:
        now = now or (lambda: datetime.now(timezone.utc))
        while now() < parse_time(plan.deadline):
            if any(row['name'] == plan.label and row['status'] == 'online' for row in runners(config, github)):
                break
            sleep(5)
        else:
            raise TimeoutError(f'runner {plan.label} did not come online before deadline')
    return {'instance_id': instance_id, 'label': plan.label, 'image_id': image_id,
            'deadline': plan.deadline, 'test_timeout_minutes': plan.test_timeout_minutes,
            'plan': plan.to_dict(), 'sha': sha, 'mode': mode}
