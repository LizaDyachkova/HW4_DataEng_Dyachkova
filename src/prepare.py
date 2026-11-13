"""Отбор исходного CSV и независимый эталон пар с правилами ДЗ3."""
import csv, json, hashlib, sys
from pathlib import Path
from collections import Counter
from itertools import combinations
import hw3_common as common
root=Path(__file__).resolve().parents[1]
out=root/'data'; out.mkdir(exist_ok=True)
source=Path(sys.argv[1])
sha=hashlib.sha256()
with source.open('rb') as f:
    for block in iter(lambda:f.read(8*1024*1024), b''): sha.update(block)
pairs=Counter(); matched=rows=0
with source.open(encoding='utf-8-sig',newline='') as f, (out/'skills.jsonl').open('w') as target:
    for row in csv.DictReader(f):
        rows+=1
        if row['role']!=common.ROLE or not common.matches_variant(row['name'],row['key_skills']): continue
        skills=common.parse_skills(row['key_skills']); matched+=1
        target.write(json.dumps(skills,ensure_ascii=False)+'\n')
        pairs.update(combinations(skills,2))
items=[[a,b,n] for (a,b),n in sorted(pairs.items())]
body=json.dumps(items,ensure_ascii=False,separators=(',',':')).encode()
(out/'baseline_pairs.json').write_bytes(body)
summary=dict(rows_scanned=rows,vacancies_matched=matched,distinct_pairs=len(pairs),occurrences_pairs=sum(pairs.values()),pairs_sha256=hashlib.sha256(body).hexdigest(),source_sha256=sha.hexdigest(),source_size=source.stat().st_size)
assert (matched,len(pairs),sum(pairs.values()))==(6076,33585,158841), summary
(out/'baseline.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2))
print(json.dumps(summary,ensure_ascii=False,indent=2))
