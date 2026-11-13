"""Две копии, ephemeral sequential election, чекпоинты и fenced sink."""
import argparse,hashlib,json,logging,os,time
from collections import Counter
from itertools import combinations
from pathlib import Path
from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError,NodeExistsError,SessionExpiredError,ConnectionLoss

ROOT=Path(__file__).resolve().parents[1]
HOSTS='127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183'
def encode(x): return json.dumps(x,ensure_ascii=False,separators=(',',':')).encode()
def now(): return time.clock_gettime(time.CLOCK_MONOTONIC)
def event(kind,**kw):
    record=dict(event=kind,monotonic=now(),utc=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),pid=os.getpid(),**kw)
    print(json.dumps(record,ensure_ascii=False),flush=True)
def checked(tx):
    results=tx.commit()
    for result in results:
        if isinstance(result,Exception): raise result
    return results

def publish(hosts,base,token,result,fencing):
    # Получатель открывает отдельную сессию. Сессия вычислителя может истечь.
    sink=KazooClient(hosts=hosts,timeout=10); sink.start(timeout=20)
    try:
        tx=sink.transaction()
        if fencing: tx.check(base+'/epoch',version=token)
        tx.set_data(base+'/result',encode(dict(result,epoch=token)),version=-1)
        # Аудит и указатель фиксируются одной транзакцией.
        tx.create(base+'/writes/write-',encode(dict(result,epoch=token)),sequence=True)
        checked(tx)
        _,stat=sink.get(base+'/result')
        event('записал результат',epoch=token,pointer_version=stat.version,**result)
    finally: sink.stop(); sink.close()

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--run',required=True); ap.add_argument('--name',required=True)
    ap.add_argument('--hosts',default=HOSTS); ap.add_argument('--delay',type=float,default=0.8)
    ap.add_argument('--batch',type=int,default=200); ap.add_argument('--fencing',type=int,choices=[0,1],default=int(os.getenv('FENCING','0')))
    args=ap.parse_args(); base='/hw4/'+args.run
    zk=KazooClient(hosts=args.hosts,timeout=10)
    zk.add_listener(lambda state:event('session',state=state))
    zk.start(timeout=20)
    try:
        for suffix in ('/election','/writes'): zk.ensure_path(base+suffix)
        for suffix in ('/epoch','/checkpoint','/result'):
            try: zk.create(base+suffix,b'{}')
            except NodeExistsError: pass
        candidate=zk.create(base+'/election/c-',encode(dict(name=args.name)),ephemeral=True,sequence=True)
        own=candidate.rsplit('/',1)[1]
        while True:
            children=sorted(zk.get_children(base+'/election'))
            if own not in children: raise RuntimeError('Сессия истекла до получения лидерства')
            idx=children.index(own)
            if idx==0: break
            import threading
            changed=threading.Event()
            if zk.exists(base+'/election/'+children[idx-1],watch=lambda _:changed.set()): changed.wait(1)
        _,es=zk.get(base+'/epoch')
        token=zk.set(base+'/epoch',encode(dict(name=args.name)),version=es.version).version
        # Лидерство начинается при успешном получении новой эпохи.
        event('leader',name=args.name,epoch=token,session_timeout_ms=zk._session_timeout)
        cp_raw,_=zk.get(base+'/checkpoint'); cp=json.loads(cp_raw)
        counts=Counter(); offset=0
        if cp:
            payload=(ROOT/'state'/args.run/cp['file']).read_bytes()
            assert hashlib.sha256(payload).hexdigest()==cp['sha256']
            data=json.loads(payload); offset=data['offset']
            counts.update({tuple(k):v for k,v in data['pairs']})
        event('restore',name=args.name,offset=offset,epoch=token)
        skills=[json.loads(s) for s in (ROOT/'data/skills.jsonl').read_text().splitlines()]
        state=ROOT/'state'/args.run; state.mkdir(parents=True,exist_ok=True)
        for start in range(offset,len(skills),args.batch):
            for row in skills[start:start+args.batch]: counts.update(combinations(row,2))
            offset=min(start+args.batch,len(skills))
            # Задержка даёт воспроизводимое окно для SIGSTOP в быстром расчёте.
            time.sleep(args.delay)
            payload=encode(dict(offset=offset,pairs=[[list(k),v] for k,v in sorted(counts.items())]))
            digest=hashlib.sha256(payload).hexdigest(); filename=digest+'.json'
            target=state/filename
            if not target.exists():
                tmp=state/(filename+'.'+str(os.getpid())+'.tmp'); tmp.write_bytes(payload); tmp.replace(target)
            try:
                tx=zk.transaction(); tx.check(base+'/epoch',version=token)
                tx.set_data(base+'/checkpoint',encode(dict(offset=offset,file=filename,sha256=digest)))
                checked(tx); event('checkpoint',name=args.name,offset=offset,epoch=token)
            except (BadVersionError,SessionExpiredError,ConnectionLoss):
                event('stale_checkpoint_rejected',name=args.name,offset=offset,epoch=token)
        items=[[a,b,n] for (a,b),n in sorted(counts.items())]; body=encode(items)
        baseline=json.loads((ROOT/'data/baseline.json').read_text())
        assert body==(ROOT/'data/baseline_pairs.json').read_bytes(), 'Все пары и счётчики должны совпасть'
        result=dict(name=args.name,distinct_pairs=len(counts),occurrences_pairs=sum(counts.values()),vacancies_matched=offset,pairs_sha256=hashlib.sha256(body).hexdigest())
        assert result['pairs_sha256']==baseline['pairs_sha256']
        resultfile=state/(args.name+'-pairs.json'); resultfile.write_bytes(body)
        event('verified',epoch=token,**result)
        try: publish(args.hosts,base,token,result,bool(args.fencing))
        except BadVersionError: event('BadVersionError',name=args.name,epoch=token,reason='Получатель отклонил устаревшую эпоху')
    finally: zk.stop(); zk.close()
if __name__=='__main__':
    logging.basicConfig(level=logging.WARNING)
    main()
