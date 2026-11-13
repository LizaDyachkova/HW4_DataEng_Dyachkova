"""Сохранить пути к Java и ZooKeeper без изменения системных настроек."""
import json,subprocess,sys,shutil
from pathlib import Path
root=Path(__file__).resolve().parents[1]
if len(sys.argv)!=3: raise SystemExit('python scripts/configure.py JAVA_HOME ZOOKEEPER_HOME')
java=Path(sys.argv[1]).resolve();zk=Path(sys.argv[2]).resolve()
assert (java/'bin/java').is_file() and (zk/'bin/zkServer.sh').is_file()
p=subprocess.run([str(java/'bin/java'),'-version'],capture_output=True,text=True,check=True)
print(p.stderr)
(root/'runtime').mkdir(exist_ok=True)
(root/'runtime/paths.json').write_text(json.dumps(dict(java_home=str(java),zk_home=str(zk)),indent=2))

for i in (1,2,3):
    target=root/'runtime'/('zk'+str(i))
    if not target.exists(): shutil.copytree(zk,target)
