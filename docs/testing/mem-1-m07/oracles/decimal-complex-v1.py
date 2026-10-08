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

"""Independent exact-literal oracle for the pinned Decimal nested fixture.

No database/golden input. Types come from actual DDL; recursive nullable casts
use precision/scale, scalar arithmetic uses the existing capped Decimal128
result domain. Map enumeration sorts keys; map rendering preserves entry order.
"""
import argparse
import hashlib
import json
import re
from decimal import Decimal, ROUND_HALF_UP, localcontext
from pathlib import Path

SQL_SHA256 = '40ead3548aa1b351ab764a29d6723acc79a8905d4230658abad6a0edfe15c168'

def require(condition, message):
    if not condition:
        raise ValueError(message)

class Map:
    def __init__(self, entries):
        self.entries = entries
    def get(self, key):
        return next((value for item, value in self.entries if item == key), None)
    def sorted(self):
        require(all(k is not None for k, _ in self.entries), 'unexpected nullable map key')
        return sorted(self.entries)

class Parser:
    def __init__(self, source):
        token = r"\s*(NULL|MAP|ROW|'[^']*'|-?\d+(?:\.\d+)?|[\[\]{}(),:])"
        self.tokens=[]
        while source.strip():
            found=re.match(token, source)
            require(found, f'unsupported literal: {source[:80]}')
            self.tokens.append(found[1]); source=source[found.end():]
        self.i=0
    def pop(self, expected=None):
        require(self.i < len(self.tokens), 'unexpected end of literal')
        token=self.tokens[self.i]; self.i+=1
        require(expected is None or token==expected, f'expected {expected}, got {token}')
        return token
    def peek(self):
        return self.tokens[self.i] if self.i < len(self.tokens) else None
    def sequence(self, closing):
        items=[]
        if self.peek()!=closing:
            while True:
                items.append(self.value())
                if self.peek()!=',': break
                self.pop(',')
        self.pop(closing)
        return items
    def value(self):
        token=self.pop()
        if token=='NULL': return None
        if token=='[': return self.sequence(']')
        if token=='ROW': self.pop('('); return tuple(self.sequence(')'))
        if token=='(' : return tuple(self.sequence(')'))
        if token=='MAP':
            self.pop('{'); items=[]
            if self.peek()!='}':
                while True:
                    key=self.value(); self.pop(':'); items.append((key,self.value()))
                    if self.peek()!=',': break
                    self.pop(',')
            self.pop('}'); return Map(items)
        if token.startswith("'"): return token[1:-1]
        return Decimal(token) if '.' in token else int(token)
    def tuples(self):
        rows=[]
        while self.peek() is not None:
            rows.append(self.value())
            if self.peek() is not None: self.pop(',')
        return rows

def split_outer(text):
    depth=0; start=0; output=[]
    for i, char in enumerate(text):
        if char in '<(': depth+=1
        elif char in '>)': depth-=1
        elif char==',' and depth==0:
            output.append(text[start:i].strip()); start=i+1
    require(depth==0, 'unbalanced type declaration')
    output.append(text[start:].strip())
    return output

def field(text):
    name, dtype=text.split(None,1)
    return name, type_spec(dtype)

def type_spec(text):
    text=text.strip()
    if text in ('INT','BIGINT','STRING'): return text
    m=re.fullmatch(r'DECIMAL\((\d+),(\d+)\)',text)
    if m: return ('decimal',int(m[1]),int(m[2]))
    m=re.fullmatch(r'(ARRAY|MAP|STRUCT)<(.*)>',text,re.S)
    require(m, f'unknown DDL type {text}')
    children=split_outer(m[2])
    if m[1]=='ARRAY': require(len(children)==1,'array child width'); return ('array',type_spec(children[0]))
    if m[1]=='MAP': require(len(children)==2,'map child width'); return ('map',*[type_spec(c) for c in children])
    return ('struct',[field(c) for c in children])

def decimal_cast(value, precision, scale):
    if value is None: return None
    value=Decimal(value).quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)
    return value if abs(value) < Decimal(10)**(precision-scale) else None

def cast(value, dtype):
    if value is None: return None
    if dtype in ('INT','BIGINT'): require(isinstance(value,int),'integer literal required'); return value
    if dtype=='STRING': require(isinstance(value,str),'string literal required'); return value
    if dtype[0]=='decimal': return decimal_cast(value,*dtype[1:])
    if dtype[0]=='array': require(isinstance(value,list),'array literal required'); return [cast(v,dtype[1]) for v in value]
    if dtype[0]=='map':
        require(isinstance(value,Map),'map literal required')
        entries=[(cast(k,dtype[1]),cast(v,dtype[2])) for k,v in value.entries]
        require(all(k is not None for k,_ in entries),'map key precision overflow')
        return Map(entries)
    require(dtype[0]=='struct' and isinstance(value,tuple) and len(value)==len(dtype[1]),'struct layout mismatch')
    return {name:cast(v,t) for v,(name,t) in zip(value,dtype[1])}

def render(value, nested=False):
    if value is None: return 'null' if nested else 'NULL'
    if isinstance(value,Decimal): return format(value,'f')
    if isinstance(value,str): return json.dumps(value,separators=(',',':')) if nested else value
    if isinstance(value,int): return str(value)
    if isinstance(value,list): return '['+','.join(render(v,True) for v in value)+']'
    if isinstance(value,Map): return '{'+','.join(render(k,True)+':'+render(v,True) for k,v in value.entries)+'}'
    require(isinstance(value,dict),'unknown rendering value')
    return '{'+','.join(json.dumps(k)+':'+render(v,True) for k,v in value.items())+'}'

def element(values,index):
    return values[index-1] if 1<=index<=len(values) else None

def derive(source):
    require(hashlib.sha256(source.encode()).hexdigest()==SQL_SHA256,'frozen SQL changed; oracle review required')
    setup=re.sub(r'--[^\n]*','',re.split(r'(?m)^-- query 2\n',source)[0])
    schemas={name:[field(c) for c in split_outer(body)] for name,body in re.findall(r'CREATE TABLE \$\{case_db\}\.(\w+)\s*\((.*?)\)\s*TBLPROPERTIES',setup,re.S)}
    require(len(schemas)==4,'schema count changed')
    tables={}
    for name,body in re.findall(r'INSERT INTO \$\{case_db\}\.(\w+) VALUES\s*(.*?);',setup,re.S):
        schema=schemas[name]; rows=Parser(body).tuples()
        require(all(len(r)==len(schema) for r in rows),'table tuple width')
        tables[name]=[{n:cast(v,t) for v,(n,t) in zip(row,schema)} for row in rows]
    require([len(tables[n]) for n in schemas]==[5,4,4,2],'setup row count changed')
    labels={int(n):re.search(r"SELECT\s+'([^']+)' as test_name",body)[1] for n,body in re.findall(r'(?ms)^-- query (\d+)\n(.*?)(?=^-- query |\Z)',source) if int(n)>1}
    headers={2:'test_name id decimal_array_50 array_size first_element last_element',3:'test_name id simple_decimals first_decimal second_decimal third_decimal',4:'test_name id decimal_map_50 map_size price_value all_keys all_values',5:'test_name id decimal_map_76 map_size large_value1 negative_value',6:'test_name id key_decimal_map map_size value_for_key decimal_keys',7:'test_name id financial_data balance credit_limit interest_rate total_available',8:'test_name id account_info account_id account_balance created_date last_transaction net_balance',9:'test_name id large_numbers max_val min_val precision_val range_type',10:'test_name id portfolio_size first_asset first_quantity first_price first_total_value first_daily_change',11:'test_name id metrics_count volatility_points first_volatility',12:'test_name id first_asset_metadata metadata_keys metadata_values'}
    results={n:(h.split(),[]) for n,h in headers.items()}
    def add(n,row,cells): results[n][1].append([labels[n],str(row['id']),*[render(v) for v in cells]])
    for row in tables['decimal_array_test']:
        arr=row['decimal_array_50']; add(2,row,[arr,len(arr),element(arr,1),element(arr,len(arr))])
        arr=row['simple_decimals']
        if len(arr)>=2: add(3,row,[arr,*[element(arr,i) for i in (1,2,3)]])
    for row in tables['decimal_map_test']:
        m=row['decimal_map_50']; sorted_items=m.sorted()
        add(4,row,[m,len(m.entries),m.get('price'),[k for k,v in sorted_items],[v for k,v in sorted_items]])
        m=row['decimal_map_76']
        if m.entries: add(5,row,[m,len(m.entries),m.get('large_num1'),m.get('negative')])
        m=row['key_decimal_map']
        if m.entries: add(6,row,[m,len(m.entries),m.get(Decimal('1234567890123456789012345678.1234567890')),[k for k,v in m.sorted()]])
    for row in tables['decimal_struct_test']:
        f=row['financial_data']
        if f is not None:
            total=decimal_cast(f['balance']+f['credit_limit'],38,15)
            add(7,row,[f,f['balance'],f['credit_limit'],f['interest_rate'],total])
        a=row['account_info']; meta=a['metadata']
        # Mixed DECIMAL(38,15)/(38,10) subtract aligns to scale15, then
        # caps output precision at38; the large integer results become NULL.
        net=decimal_cast(a['balance']-meta['last_transaction'],38,15)
        add(8,row,[a,a['account_id'],a['balance'],meta['created_date'],meta['last_transaction'],net])
        n=row['large_numbers']
        if n is not None:
            group='MIXED_RANGE' if n['max_value']>0 and n['min_value']<0 else 'POSITIVE_RANGE' if n['max_value']>0 else 'OTHER'
            add(9,row,[n,n['max_value'],n['min_value'],n['precision_value'],group])
    for row in tables['complex_nested_test']:
        portfolio=row['portfolio']; a=portfolio[0]; m=a['metadata']; risk=row['risk_metrics']
        unscaled=int(a['quantity']*10**15)*int(a['price']*10**15)
        require(abs(unscaled)>=2**127 and abs(unscaled)>=10**38,'product overflow premise changed')
        add(10,row,[len(portfolio),a['asset_name'],a['quantity'],a['price'],None,m.get('daily_change')])
        arr=risk.get('volatility')
        if arr is not None: add(11,row,[len(risk.entries),len(arr),element(arr,1)])
        if portfolio: add(12,row,[m,[k for k,v in m.sorted()],[v for k,v in m.sorted()]])
    return results

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('sql','output','audit'): parser.add_argument('--'+name,type=Path,required=True)
    args=parser.parse_args()
    with localcontext() as context:
        context.prec=128
        results=derive(args.sql.read_text())
    text='\n\n'.join(f'-- query {n}\n'+'\t'.join(h)+'\n'+'\n'.join('\t'.join(r) for r in rows) for n,(h,rows) in results.items())+'\n'
    args.output.parent.mkdir(parents=True,exist_ok=True); args.output.write_text(text)
    args.audit.write_text(json.dumps({'schema_version':1,'sql_sha256':SQL_SHA256,'oracle_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'result_sha256':hashlib.sha256(text.encode()).hexdigest(),'queries':len(results),'rows':sum(len(r) for h,r in results.values()),'basis':'Original DDL and recursively parsed exact literal text; Decimal precision128, nullable precision/scale cast, capped arithmetic, sorted map enumeration, nested text rendering. No database/golden input.'},indent=2)+'\n')
    print(f'derived {len(results)} queries')

if __name__=='__main__': main()
