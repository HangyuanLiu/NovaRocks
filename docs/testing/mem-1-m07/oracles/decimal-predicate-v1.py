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

"""Independent oracle for the pinned Decimal predicate fixture.

Exact literal parsing and Python Decimal arithmetic; SQL predicates follow
three-valued logic. The closed parser rejects unknown syntax and the full SQL
hash pins setup, queries, aliases and order clauses. No database/golden input.
"""
import argparse
import functools
import hashlib
import json
import re
from decimal import Decimal, ROUND_HALF_UP, localcontext
from pathlib import Path

SQL_SHA256='8b8c608fd31122e4ca2ce934c391174b16d41295668279f4fde2139557970989'
COLUMNS=['id','big_decimal','huge_decimal','max_decimal']
SCALES={'big_decimal':15,'huge_decimal':20,'max_decimal':0}

def require(condition,message):
    if not condition: raise ValueError(message)

def conjunction(a,b):
    if a is False or b is False:return False
    if a is None or b is None:return None
    return True

def disjunction(a,b):
    if a is True or b is True:return True
    if a is None or b is None:return None
    return False

def negate(a):return None if a is None else not a

class Parser:
    def __init__(self,text):
        self.tokens=[]
        pattern=r"\s*('[^']*'|-?\d+(?:\.\d+)?|[A-Za-z_][A-Za-z_0-9]*|>=|<=|<>|!=|[(),*=<>])"
        while text.strip():
            m=re.match(pattern,text); require(m,f'unknown expression syntax {text[:50]}')
            self.tokens.append(m[1]);text=text[m.end():]
        self.i=0
    def peek(self):return self.tokens[self.i].upper() if self.i<len(self.tokens) else None
    def pop(self,expected=None):
        require(self.i<len(self.tokens),'unexpected expression end')
        t=self.tokens[self.i];self.i+=1
        require(expected is None or t.upper()==expected,f'expected {expected}, got {t}')
        return t
    def complete(self):
        node=self.or_expr();require(self.peek() is None,'unconsumed expression');return node
    def or_expr(self):
        x=self.and_expr()
        while self.peek()=='OR':self.pop();x=('or',x,self.and_expr())
        return x
    def and_expr(self):
        x=self.comparison()
        while self.peek()=='AND':self.pop();x=('and',x,self.comparison())
        return x
    def comparison(self):
        if self.peek()=='NOT':self.pop();return ('not',self.comparison())
        x=self.primary()
        if self.peek() in ('=','>','<','>=','<=','<>','!='):return (self.pop(),x,self.primary())
        if self.peek()=='IS':
            self.pop(); negative=self.peek()=='NOT'
            if negative:self.pop()
            self.pop('NULL');return ('is_not_null' if negative else 'is_null',x)
        negative=self.peek()=='NOT'
        if negative:self.pop()
        if self.peek()=='IN':
            self.pop();self.pop('(');items=[self.primary()]
            while self.peek()==',':self.pop();items.append(self.primary())
            self.pop(')');node=('in',x,items)
        elif self.peek()=='BETWEEN':
            self.pop();low=self.primary();self.pop('AND');high=self.primary();node=('between',x,low,high)
        else:
            require(not negative,'unknown NOT suffix');return x
        return ('not',node) if negative else node
    def primary(self):
        t=self.pop();word=t.upper()
        if t=='(':
            node=self.or_expr();self.pop(')');return node
        if t.startswith("'"):return ('literal',t[1:-1])
        if word=='NULL':return ('literal',None)
        if re.fullmatch(r'-?\d+(?:\.\d+)?',t):return ('literal',Decimal(t) if '.' in t else int(t))
        if word=='CASE':
            self.pop('WHEN');cond=self.or_expr();self.pop('THEN');yes=self.primary();self.pop('ELSE');no=self.primary();self.pop('END');return ('case',cond,yes,no)
        if word in ('COUNT','SUM','AVG','MIN','MAX','TYPEOF'):
            self.pop('(')
            if self.peek()=='*':self.pop();arg=('star',)
            else:arg=self.or_expr()
            self.pop(')');return ('function',word,arg)
        require(t.lower() in COLUMNS,f'unknown column {t}')
        return ('column',t.lower())

def evaluate(node,row,group=None):
    op=node[0]
    if op=='literal':return node[1]
    if op=='column':return row[node[1]]
    if op=='star':return 1
    if op in ('and','or'):
        a=evaluate(node[1],row,group);b=evaluate(node[2],row,group)
        return conjunction(a,b) if op=='and' else disjunction(a,b)
    if op=='not':return negate(evaluate(node[1],row,group))
    if op in ('is_null','is_not_null'):
        null=evaluate(node[1],row,group) is None
        return null if op=='is_null' else not null
    if op=='case':return evaluate(node[2] if evaluate(node[1],row,group) is True else node[3],row,group)
    if op in ('=','>','<','>=','<=','<>','!='):
        a=evaluate(node[1],row,group);b=evaluate(node[2],row,group)
        if a is None or b is None:return None
        if op=='=':return a==b
        if op=='>':return a>b
        if op=='<':return a<b
        if op=='>=':return a>=b
        if op=='<=':return a<=b
        return a!=b
    if op=='in':
        x=evaluate(node[1],row,group);values=[evaluate(n,row,group) for n in node[2]]
        if x is None:return None
        if any(v is not None and x==v for v in values):return True
        return None if None in values else False
    if op=='between':return conjunction(evaluate(('>=',node[1],node[2]),row,group),evaluate(('<=',node[1],node[3]),row,group))
    require(op=='function',f'unknown node {op}')
    name,arg=node[1:]
    if name=='TYPEOF':
        require(arg[0]=='column' and arg[1] in SCALES,'unknown TYPEOF argument')
        return f'decimal128(38, {SCALES[arg[1]]})'
    require(group is not None,'aggregate outside group')
    values=[evaluate(arg,r) for r in group];values=[v for v in values if v is not None]
    if name=='COUNT':return len(values)
    if not values:return None
    if name=='MIN':return min(values)
    if name=='MAX':return max(values)
    total=sum(values)
    if name=='SUM':return total
    require(name=='AVG' and arg[0]=='column' and arg[1] in SCALES,'unknown AVG shape')
    scale=SCALES[arg[1]];scale=scale+6 if scale<=6 else 12 if scale<=12 else scale
    return (total/len(values)).quantize(Decimal(1).scaleb(-scale),rounding=ROUND_HALF_UP)

def split_items(text):
    depth=0;quoted=False;start=0;out=[]
    for i,c in enumerate(text):
        if c=="'":quoted=not quoted
        if not quoted:
            if c=='(':depth+=1
            elif c==')':depth-=1
            elif c==',' and depth==0:out.append(text[start:i].strip());start=i+1
    require(not quoted and depth==0,'unbalanced SELECT list')
    out.append(text[start:].strip());return out

def aggregate(node):
    return node[0]=='function' and node[1]!='TYPEOF'

def render(value):
    if value is None:return 'NULL'
    return format(value,'f') if isinstance(value,Decimal) else str(value)

def derive(source):
    require(hashlib.sha256(source.encode()).hexdigest()==SQL_SHA256,'frozen SQL changed; oracle review required')
    setup=re.sub(r'--[^\n]*','',re.split(r'(?m)^-- query 2\n',source)[0]);rows=[]
    declarations=re.findall(r'(big_decimal|huge_decimal|max_decimal) decimal\((\d+),(\d+)\)',setup)
    require(declarations==[(n,'38',str(s)) for n,s in SCALES.items()],'actual DDL changed')
    inserts=re.findall(r'INSERT INTO \$\{case_db\}\.decimal_test VALUES\s*(.*?);',setup,re.S)
    require(len(inserts)==4,'insert count changed')
    for body in inserts:
        for tup in re.findall(r'\(([^()]*)\)',body):
            parts=[v.strip() for v in tup.split(',')];require(len(parts)==4,'tuple width changed')
            row={'id':int(parts[0])}
            for col,text in zip(COLUMNS[1:],parts[1:]):
                value=None if text=='NULL' else Decimal(text)
                if value is not None:
                    value=value.quantize(Decimal(1).scaleb(-SCALES[col]));require(abs(value)<Decimal(10)**(38-SCALES[col]),'unexpected setup overflow')
                row[col]=value
            rows.append(row)
    require([r['id'] for r in rows]==list(range(1,26)),'setup identities changed')
    result={}
    for number,body in re.findall(r'(?ms)^-- query (\d+)\n(.*?)(?=^-- query |\Z)',source):
        n=int(number)
        if n==1:continue
        sql=re.sub(r'--[^\n]*','',body).strip()
        m=re.fullmatch(r'SELECT\s+(.*?)\s+FROM \$\{case_db\}\.decimal_test(.*);',sql,re.S);require(m,f'unknown query {n}')
        selected=m[1].strip();clauses={}
        markers=list(re.finditer(r'\b(WHERE|GROUP BY|HAVING|ORDER BY)\b',m[2]))
        require(not m[2][:markers[0].start()].strip() if markers else not m[2].strip(),'unparsed query suffix')
        for i,marker in enumerate(markers):clauses[marker[1]]=m[2][marker.end():markers[i+1].start() if i+1<len(markers) else len(m[2])].strip()
        where=Parser(clauses['WHERE']).complete() if 'WHERE' in clauses else None
        filtered=[r for r in rows if where is None or evaluate(where,r) is True]
        if selected=='*':headers=COLUMNS.copy();exprs=[('column',c) for c in COLUMNS]
        else:
            headers=[];exprs=[]
            for item in split_items(selected):
                alias=re.fullmatch(r'(.*?)\s+as\s+(\w+)',item,re.S|re.I)
                expression=alias[1] if alias else item
                headers.append(alias[2] if alias else re.sub(r'\s+','',item).lower())
                exprs.append(Parser(expression).complete())
        if 'GROUP BY' in clauses:
            keys=[Parser(t).complete() for t in split_items(clauses['GROUP BY'])];groups={}
            for r in filtered:groups.setdefault(tuple(evaluate(k,r) for k in keys),[]).append(r)
            contexts=[(g[0],g) for key,g in sorted(groups.items())]
        elif any(aggregate(e) for e in exprs):contexts=[(filtered[0] if filtered else {},filtered)]
        else:contexts=[(r,None) for r in filtered]
        having=Parser(clauses['HAVING']).complete() if 'HAVING' in clauses else None
        output=[([evaluate(e,r,g) for e in exprs],r) for r,g in contexts if having is None or evaluate(having,r,g) is True]
        if 'ORDER BY' in clauses:
            terms=[]
            for term in split_items(clauses['ORDER BY']):
                match=re.fullmatch(r'(\w+)(?: (ASC|DESC))?(?: NULLS (FIRST|LAST))?',term);require(match,f'unknown sort {term}')
                name,dir_,nulls=match.groups();desc=dir_=='DESC';null_first=(nulls=='FIRST') if nulls else not desc
                require(name in headers or name in COLUMNS,'unknown sort name')
                terms.append((name,desc,null_first))
            def compare(a,b):
                for name,desc,null_first in terms:
                    x=a[0][headers.index(name)] if name in headers else a[1][name]
                    y=b[0][headers.index(name)] if name in headers else b[1][name]
                    if x is None or y is None:
                        c=0 if x is None and y is None else (-1 if null_first else 1) if x is None else (1 if null_first else -1)
                    else:c=((x>y)-(x<y))*(-1 if desc else 1)
                    if c:return c
                return 0
            output.sort(key=functools.cmp_to_key(compare))
        result[n]=(headers,[[render(v) for v in cells] for cells,r in output])
    require(list(result)==list(range(2,174)),'query set changed')
    return result

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('sql','output','audit'):parser.add_argument('--'+name,type=Path,required=True)
    args=parser.parse_args()
    with localcontext() as ctx:
        ctx.prec=128;results=derive(args.sql.read_text())
    text='\n\n'.join(f'-- query {n}\n'+'\t'.join(h)+'\n'+'\n'.join('\t'.join(row) for row in rows) for n,(h,rows) in results.items())+'\n'
    args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(text)
    args.audit.write_text(json.dumps({'schema_version':1,'sql_sha256':SQL_SHA256,'oracle_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'result_sha256':hashlib.sha256(text.encode()).hexdigest(),'queries':len(results),'rows':sum(len(rows) for h,rows in results.values()),'basis':'Exact 25 literal rows and actual DDL; independent three-valued predicate logic, Decimal128 sum/AVG HALF_UP scales, NULL grouping/count, sort clauses. SQL runner existing order_sensitive=false compares exact multisets; this does not prove tied-row order. No database/golden input.'},indent=2)+'\n')
    print(f'derived {len(results)} queries')
if __name__=='__main__':main()
