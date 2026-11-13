"""Реальные quorum/ephemeral/SIGSTOP эксперименты, без подставных логов."""
import argparse,json,os,signal,subprocess,sys,time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from coordinator import HOSTS,encode,now
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError
import ensemble
ROOT=Path(__file__).resolve().parents[1]

def client(hosts=HOSTS):
    z=KazooClient(hosts=hosts,timeout=10); z.start(timeout=20); return z

def wait_for(predicate,timeout=90):
    end=now()+timeout
    while now()<end:
        value=predicate()
        if value: return value
        time.sleep(.1)
    raise TimeoutError('Не выполнено условие эксперимента')

def events(path):
    result=[]
    if not path.exists(): return result
    for line in path.read_text().splitlines():
        try: result.append(json.loads(line))
        except ValueError: pass
    return result

def quorum():
    logfile=ROOT/'logs/ensemble.txt'
    with logfile.open('w') as log:
        def record(s): print(s,flush=True); log.write(s+'\n'); log.flush()
        time.sleep(3)
        initial={i:ensemble.status(i) for i in (1,2,3)}
        for i,s in initial.items(): record(f'INITIAL server {i}\n{s}')
        assert sum('Mode: leader' in s for s in initial.values())==1
        assert sum('Mode: follower' in s for s in initial.values())==2
        followers=[i for i,s in initial.items() if 'Mode: follower' in s]
        first,second=followers
        z=client()
        if z.exists('/hw4/quorum'): z.delete('/hw4/quorum',recursive=True)
        z.ensure_path('/hw4/quorum')
        ensemble.action('stop',[first]); time.sleep(2)
        z.create('/hw4/quorum/one-down',b'ok')
        record('ONE SERVER DOWN: create /hw4/quorum/one-down SUCCESS')
        for i in (1,2,3): record(f'ONE DOWN server {i}\n{ensemble.status(i)}')
        ensemble.action('stop',[second]); time.sleep(3)
        for i in (1,2,3): record(f'TWO DOWN server {i}\n{ensemble.status(i)}')
        # Отдельная сессия на оставшийся сервер не должна установиться.
        remaining=next(i for i in (1,2,3) if i not in followers)
        probe=KazooClient(hosts=f'127.0.0.1:{2180+remaining}',timeout=4)
        try:
            probe.start(timeout=5)
            raise AssertionError('Ожидалось отсутствие кворума')
        except KazooTimeoutError: record('TWO SERVERS DOWN: client connection TIMEOUT, quorum unavailable')
        finally: probe.stop(); probe.close()
        ensemble.action('start',followers); z.stop(); z.close(); time.sleep(3)
        z=client()
        z.create('/hw4/quorum/recovered',b'ok'); record('RESTORED: create SUCCESS')
        for i in (1,2,3): record(f'RESTORED server {i}\n{ensemble.status(i)}')
        z.stop(); z.close()
        # CLI-опыт: первая CLI создаёт ephemeral и удерживает сессию.
        e,home=ensemble.env()
        cli=[str(home/'bin/zkCli.sh'),'-server','127.0.0.1:2182']
        with (ROOT/'logs/ephemeral-creator.txt').open('w') as out:
            creator=subprocess.Popen(cli,env=e,stdin=subprocess.PIPE,stdout=out,stderr=subprocess.STDOUT,text=True)
            creator.stdin.write('create -e /hw4-ephemeral alive\n'); creator.stdin.flush()
            observer=client(); wait_for(lambda:observer.exists('/hw4-ephemeral'))
            p=subprocess.run(cli,input='get /hw4-ephemeral\nquit\n',env=e,capture_output=True,text=True,timeout=20)
            (ROOT/'logs/ephemeral-observer-before.txt').write_text(p.stdout+p.stderr)
            record('EPHEMERAL before quit: '+str(observer.exists('/hw4-ephemeral')))
            creator.stdin.write('quit\n');creator.stdin.flush();creator.wait(timeout=15)
            wait_for(lambda:observer.exists('/hw4-ephemeral') is None)
            record('EPHEMERAL after creator quit: None (deleted)')
            p=subprocess.run(cli,input='get /hw4-ephemeral\nquit\n',env=e,capture_output=True,text=True,timeout=20)
            (ROOT/'logs/ephemeral-observer-after.txt').write_text(p.stdout+p.stderr)
            observer.stop();observer.close()

def failover(fencing):
    run=('fenced' if fencing else 'unfenced')+'-'+time.strftime('%Y%m%d-%H%M%S')
    logs=ROOT/'logs'/run;logs.mkdir()
    proc=[];handles=[]
    def spawn(name):
        path=logs/(name+'.jsonl'); handle=path.open('w'); handles.append(handle)
        p=subprocess.Popen([sys.executable,str(ROOT/'src/coordinator.py'),'--run',run,'--name',name,'--fencing',str(fencing)],stdout=handle,stderr=subprocess.STDOUT)
        proc.append(p);return p,path
    a,pa=spawn('A'); b=None
    try:
        wait_for(lambda:any(x['event']=='leader' for x in events(pa)))
        b,pb=spawn('B')
        wait_for(lambda:any(x['event']=='checkpoint' and x['offset']>=600 for x in events(pa)))
        os.kill(a.pid,signal.SIGSTOP); stopped=now()
        print(f'{run}: SIGSTOP A pid={a.pid} monotonic={stopped}',flush=True)
        lead=wait_for(lambda:next((x for x in events(pb) if x['event']=='leader'),None),40)
        elapsed=lead['monotonic']-stopped
        print(f'{run}: B leader after {elapsed:.3f} s',flush=True)
        # Возобновление только после успешной публикации резервного координатора.
        wait_for(lambda:any(x['event']=='записал результат' for x in events(pb)),60)
        wait_for(lambda:now()-stopped>=40,45)
        os.kill(a.pid,signal.SIGCONT); resumed=now()
        a.wait(timeout=90);b.wait(timeout=20)
        assert a.returncode==0 and b.returncode==0
        z=client()
        try:
            raw,stat=z.get('/hw4/'+run+'/result'); writes=z.get_children('/hw4/'+run+'/writes')
            ea,eb=events(pa),events(pb)
            expected=1 if fencing else 2
            assert stat.version==expected and len(writes)==expected
            assert sum(x['event']=='записал результат' for x in ea+eb)==expected
            assert sum(x['event']=='BadVersionError' for x in ea)==fencing
            restore=next(x for x in eb if x['event']=='restore');assert restore['offset']>=600
            baseline=(ROOT/'data/baseline_pairs.json').read_bytes()
            for name in ('A','B'): assert (ROOT/'state'/run/(name+'-pairs.json')).read_bytes()==baseline
            summary=dict(run=run,fencing=fencing,stop_monotonic=stopped,resume_monotonic=resumed,pause_seconds=resumed-stopped,failover_seconds=elapsed,backup_restored_offset=restore['offset'],pointer_version=stat.version,accepted_writes=len(writes),pointer=json.loads(raw),full_pairs_equal=True)
            (logs/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2))
            print(json.dumps(summary,ensure_ascii=False,indent=2),flush=True)
            return summary
        finally:z.stop();z.close()
    finally:
        for p in proc:
            if p.poll() is None:
                os.kill(p.pid,signal.SIGCONT);p.terminate()
        for h in handles:h.close()

if __name__=='__main__':
    ap=argparse.ArgumentParser();ap.add_argument('step',choices=['quorum','unfenced','fenced','all']);a=ap.parse_args()
    if a.step in ('quorum','all'):quorum()
    if a.step in ('unfenced','all'):failover(0)
    if a.step in ('fenced','all'):failover(1)
