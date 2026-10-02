from pathlib import Path
import tempfile,subprocess,tomllib,json,os,hashlib
repo=Path.cwd();
expected=json.loads(Path(__file__).with_name('vendor-source-sha256.json').read_text())
for relative,digest in expected.items():
 assert hashlib.sha256((repo/'vendor'/relative).read_bytes()).hexdigest()==digest,relative
scratch=Path(tempfile.mkdtemp(prefix='m07-metadata-features-'))
(scratch/'src').mkdir(); (scratch/'src/lib.rs').write_text('pub fn marker() {}\n')
s='[package]\nname="m07-metadata-feature-probe"\nversion="0.1.0"\nedition="2021"\n\n[features]\nclient=["hyper/client","hyper/http2"]\nserver=["hyper/server","hyper/http2"]\nchannel=["tonic/channel"]\n\n[dependencies]\nhyper={path='+json.dumps(str(repo/'vendor/hyper-1.8.1'))+',default-features=false}\ntonic={path='+json.dumps(str(repo/'vendor/tonic-0.12.3'))+',default-features=false,optional=true}\n\n[patch.crates-io]\n'
for n,v in [('http','1.4.0'),('bytes','1.11.0'),('h2','0.4.12'),('hyper','1.8.1'),('tonic','0.12.3')]:
 s+=n+'={path='+json.dumps(str(repo/f'vendor/{n}-{v}'))+'}\n'
(scratch/'Cargo.toml').write_text(s); (scratch/'Cargo.lock').write_bytes((repo/'Cargo.lock').read_bytes())
env=dict(os.environ);env['CARGO_TARGET_DIR']='/tmp/m07-metadata-features-target'
for f in ['client','server','channel']:
 c=['cargo','clippy','--manifest-path',str(scratch/'Cargo.toml'),'--offline','--features',f,'--lib','--','-D','warnings']
 r=subprocess.run(c,env=env,check=False); print(f,'exit',r.returncode,flush=True)
 if r.returncode:raise SystemExit(r.returncode)

prod=tomllib.loads((repo/'Cargo.lock').read_text()); identities={(p['name'],p['version'],p.get('source')):p.get('checksum') for p in prod['package']}
private=tomllib.loads((scratch/'Cargo.lock').read_text());deps=[p for p in private['package'] if p['name']!='m07-metadata-feature-probe']
for p in deps:assert (p['name'],p['version'],p.get('source')) in identities and identities[(p['name'],p['version'],p.get('source'))]==p.get('checksum'),p
print('production dependency identities verified',len(deps));print('private feature workspace',scratch)
