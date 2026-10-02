import json,subprocess
from pathlib import Path
repo=Path.cwd()
protocol=['cargo','test','-p','novarocks-native-adapter','--locked','--offline']
for p in sorted((repo/'novarocks/native-adapter/tests').glob('native_*.rs')): protocol+=['--test',p.stem]
protocol+=['--','--test-threads=1']
checks={
 'protocol':protocol,
 'vendor-clippy':['cargo','clippy','-p','http','-p','h2','-p','hyper','-p','tonic','--lib','--locked','--offline','--','-D','warnings'],
 'native-clippy':['cargo','clippy','-p','novarocks-native-adapter','--all-targets','--locked','--offline'],
 'workspace-check':['cargo','check','--workspace','--all-targets','--locked','--offline'],
 'fmt':['cargo','fmt','--all','--','--check'],
}
paths=subprocess.check_output(['git','diff','--name-only'],text=True).splitlines();paths+=['vendor/h2-0.4.12/src/receive_header_table.rs']
checks['vendor-fmt']=['rustfmt','--edition','2021','--check','--config','skip_children=true']+[p for p in paths if p.startswith('vendor/') and p.endswith('.rs')]
receipts={}
for name,command in checks.items():
 log=Path('/tmp/m07-table-final-'+name+'.log')
 with log.open('w') as f: status=subprocess.run(command,cwd=repo,stdout=f,stderr=subprocess.STDOUT).returncode
 receipts[name]={'command':command,'exit':status,'log':str(log)}
 Path('/tmp/m07-table-final-checks.json').write_text(json.dumps(receipts,indent=2)+'\n')
 print(name+': '+str(status),flush=True)
 if status: raise SystemExit('check failed: '+name)
