"""Три локальных сервера; запуск штатным zkServer.sh."""
import argparse,json,os,socket,subprocess,time,shutil
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
def env():
    e=os.environ.copy()
    settings=json.loads((ROOT/'runtime/paths.json').read_text())
    e['JAVA_HOME']=settings['java_home']; e['ZK_SERVER_HEAP']='128'
    e['SERVER_JVMFLAGS']='-XX:ActiveProcessorCount=2 -Dzookeeper.nio.numWorkerThreads=4'
    return e,Path(settings['zk_home'])
def action(verb,ids=(1,2,3)):
    e,home=env()
    for i in ids:
        conf=ROOT/'conf'/str(i); conf.mkdir(parents=True,exist_ok=True)
        data=ROOT/'runtime/data'/str(i); data.mkdir(parents=True,exist_ok=True)
        (data/'myid').write_text(str(i))
        cfg='tickTime=2000\ninitLimit=10\nsyncLimit=5\nmaxSessionTimeout=20000\nminSessionTimeout=4000\nadmin.enableServer=false\n4lw.commands.whitelist=ruok,srvr,stat,mntr\nclientPortAddress=127.0.0.1\n'
        cfg+=f'dataDir={data}\nclientPort={2180+i}\n'
        cfg+=''.join(f'server.{j}=127.0.0.1:{2880+j}:{3880+j}\n' for j in (1,2,3))
        (conf/'zoo.cfg').write_text(cfg)
        shutil.copyfile(home/'conf/logback.xml',conf/'logback.xml')
        logs=ROOT/'logs/servers'/str(i); logs.mkdir(parents=True,exist_ok=True)
        ei=dict(e,ZOO_LOG_DIR=str(logs),ZOO_LOG4J_PROP='INFO,ROLLINGFILE')
        command=[str(ROOT/'runtime'/('zk'+str(i))/'bin/zkServer.sh'),'--config',str(conf)]
        if verb=='start':
            with (logs/'foreground.out').open('a') as out:
                p=subprocess.Popen(command+['start-foreground'],env=ei,stdout=out,stderr=subprocess.STDOUT,stdin=subprocess.DEVNULL)
            (data/'zookeeper_server.pid').write_text(str(p.pid))
            time.sleep(1)
            if p.poll() is not None: raise RuntimeError(f'Server {i} exited: {p.returncode}; see {logs}')
            print(f'SERVER {i}: zkServer.sh start-foreground PID={p.pid}',flush=True)
        else:
            p=subprocess.run(command+[verb],env=ei,text=True,capture_output=True)
            print(f'SERVER {i}: {verb} rc={p.returncode}\n{p.stdout}{p.stderr}',flush=True)
def status(i):
    try:
        with socket.create_connection(('127.0.0.1',2180+i),timeout=2) as s:
            s.sendall(b'srvr'); s.shutdown(socket.SHUT_WR); data=b''
            while True:
                part=s.recv(65536)
                if not part: break
                data+=part
        return data.decode()
    except OSError as ex: return str(ex)
if __name__=='__main__':
    ap=argparse.ArgumentParser(); ap.add_argument('verb',choices=['start','stop','status']); ap.add_argument('ids',nargs='*',type=int)
    a=ap.parse_args(); action(a.verb,a.ids or (1,2,3))
