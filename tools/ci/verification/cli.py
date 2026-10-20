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
"""Select, run, compose and resume local development verification."""
import sys

if __package__ in (None,''):
    from os.path import dirname, realpath
    # Remove the script directory before stdlib imports can resolve select.py.
    script_directory=dirname(realpath(__file__))
    sys.path[:]=[entry for entry in sys.path if realpath(entry)!=script_directory]
    sys.path.insert(0,dirname(script_directory))
    __package__='verification'

import argparse
import json
from pathlib import Path
import subprocess

from . import run
from .declarations import native_selector_requests,validate_declarations


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest='action',required=True)
    for name in ('plan','run','resume','compose','check-declarations'):
        command=sub.add_parser(name)
        command.add_argument('--root',type=Path)
        command.add_argument('--base',default='origin/main')
        command.add_argument('--prepared',type=Path,help='Existing canonical preparation; revalidated before use')
        if name=='compose':command.add_argument('--layer',choices=('dev','admission'),default='dev')
    confirm=sub.add_parser('confirm-cleanup')
    confirm.add_argument('attempt',type=Path)
    confirm.add_argument('--observation',type=Path)
    options=parser.parse_args(argv)
    repo=Path(__file__).resolve().parents[3]
    if options.action=='confirm-cleanup':
        value,path=run.cleanup_confirmation(options.attempt,options.observation)
        print(json.dumps({'record':str(path),'confirmations':value['confirmations']},indent=2))
        return 0 if value['confirmations'] and all(row['complete'] for row in value['confirmations']) else 1
    root=options.root or run.default_root(repo)
    inputs=options.prepared or run.prepare(repo,root)
    native=run.inventory(repo,inputs)
    if options.action=='check-declarations':
        listing=run.native_list(inputs,ignored='all')
        bindings=json.loads((inputs/'test-artifacts.json').read_text())
        results={}
        for request in native_selector_requests(native['metadata']):
            try:results[request['key']]=run.native_list(inputs,request['filterset'],'only')
            except Exception as error:results[request['key']]={'error':str(error)}
        value=validate_declarations(native['metadata'],listing,bindings,results)
        print(json.dumps(value,indent=2));return 0 if value['ok'] else 1
    chosen=run.selection(repo,options.base,native)
    if options.action in ('run','resume','compose'):
        variants=chosen
        if options.action=='compose' and options.layer=='admission':
            variants=dict(chosen,system_scenarios=[item for item in chosen['system_scenarios']
                                                  if item.get('default') and not item.get('paused')])
        run.prepare_system_variants(repo,native,variants,root)
    scope_values=run.scopes(repo,native,chosen)
    if options.action=='plan':
        print(json.dumps(dict(chosen,scopes=[run.scope_dict(value) for value in scope_values]),indent=2));return 0
    identities={value.key:value.identity for value in scope_values}
    if options.action=='compose':
        value,path=run.compose_root(root,scope_values,{'identities':identities},options.layer)
        print(json.dumps({'layer_ok':value['layer_ok'],'composition':str(path)},indent=2));return 0 if value['layer_ok'] else 1
    pending=None
    if options.action=='resume':
        value,_=run.compose_root(root,scope_values,{'identities':identities})
        pending=run.pending_scopes(value)
    value,path,attempt=run.execute(repo,root,native,chosen,scope_values,pending)
    print(json.dumps({'layer_ok':value['layer_ok'],'attempt':str(attempt),'composition':str(path),'execution_errors':value.get('execution_errors',[])},indent=2))
    return 0 if value['layer_ok'] else 1


if __name__=='__main__':
    try:sys.exit(main())
    except (OSError,ValueError,KeyError,subprocess.SubprocessError) as error:
        print('error: '+str(error),file=sys.stderr);sys.exit(2)
