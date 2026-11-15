
import hashlib,json,random,sqlite3,subprocess,sys,tempfile,threading,time,unittest
from pathlib import Path
from watermark_windows import stream
from watermark_windows import reconcile as reconcile_mod

def canon(events):
    return sorted(events,key=lambda e:(e[1],e[0]))

def canon_sha(events):
    # pins the documented encoding in docs/reconcile.md
    digest=hashlib.sha256()
    for eid,moment,value in events:
        digest.update(json.dumps({'id':eid,'event_time':moment,'value':value},
                                 sort_keys=True,ensure_ascii=False,separators=(',',':')).encode('utf-8'))
        digest.update(b'\n')
    return digest.hexdigest()

def brute(a,b,tol):
    # exhaustive bi-objective optimum over all increasing matchings: (max pairs, min total delta)
    from functools import lru_cache
    @lru_cache(maxsize=None)
    def f(i,j):
        if i==len(a) or j==len(b):return (0,0)
        best=max(f(i+1,j),f(i,j+1),key=lambda s:(s[0],-s[1]))
        if a[i][1]==b[j][1] and abs(a[i][0]-b[j][0])<=tol:
            c,cost=f(i+1,j+1);cand=(c+1,cost+abs(a[i][0]-b[j][0]))
            if (cand[0],-cand[1])>(best[0],-best[1]):best=cand
        return best
    return f(0,0)

def greedy(a,b,tol):
    # canonical order, each a takes the unmatched compatible b with min delta (tie: min index)
    used=[False]*len(b);count=0;cost=0
    for t,v in a:
        pick=None
        for j,(t2,v2) in enumerate(b):
            if not used[j] and v2==v and abs(t-t2)<=tol:
                key=(abs(t-t2),j)
                if pick is None or key<pick[0]:pick=(key,j)
        if pick is not None:used[pick[1]]=True;count+=1;cost+=pick[0][0]
    return (count,cost)

class ReconcileCLI(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.dir=Path(self.tmp.name)
    def make_db(self,name,events,width=86400,delay=0):
        db=self.dir/name
        stream.init(db,['p1'],width,delay)
        if events:
            batch=self.dir/(name+'.jsonl')
            batch.write_text(''.join(json.dumps({'partition':'p1','offset':k,'kind':'event','id':e[0],
                                                 'event_time':e[1],'value':e[2]})+'\n'
                                     for k,e in enumerate(events)))
            stream.apply(db,batch)
        return db
    def run_cli(self,a,b,tol,out):
        return subprocess.run([sys.executable,'-m','watermark_windows.cli','reconcile',
                               '--a',str(a),'--b',str(b),'--tolerance',str(tol),'--output',str(out)],
                              capture_output=True,text=True)
    def check_report(self,report,ea,eb,tol):
        pairs=report['pairs']
        self.assertEqual(report['schema_version'],1);self.assertEqual(report['tolerance'],tol)
        self.assertEqual(report['objective']['pair_count'],len(pairs))
        self.assertEqual(report['objective']['total_abs_time_delta'],sum(p['time_delta'] for p in pairs))
        ai=[p['a_index'] for p in pairs];bj=[p['b_index'] for p in pairs]
        self.assertEqual(ai,sorted(set(ai)));self.assertEqual(bj,sorted(set(bj)))  # strictly increasing both sides
        for p in pairs:
            a=ea[p['a_index']];b=eb[p['b_index']]
            self.assertEqual((p['a_id'],p['event_time_a'],p['value']),a)
            self.assertEqual((p['b_id'],p['event_time_b'],p['value']),b)
            self.assertEqual(p['time_delta'],abs(a[1]-b[1]));self.assertLessEqual(p['time_delta'],tol)
        ua={e['index'] for e in report['unmatched_a']};ub={e['index'] for e in report['unmatched_b']}
        self.assertEqual(ua,set(range(len(ea)))-set(ai));self.assertEqual(ub,set(range(len(eb)))-set(bj))
        for e in report['unmatched_a']:self.assertEqual((e['id'],e['event_time'],e['value']),ea[e['index']])
        for e in report['unmatched_b']:self.assertEqual((e['id'],e['event_time'],e['value']),eb[e['index']])
        self.assertEqual(report['sources']['a']['accepted_events'],len(ea))
        self.assertEqual(report['sources']['b']['accepted_events'],len(eb))
        self.assertEqual(report['sources']['a']['canonical_sha256'],canon_sha(ea))
        self.assertEqual(report['sources']['b']['canonical_sha256'],canon_sha(eb))

    def test_exhaustive_small_biobjective(self):
        rng=random.Random(128)
        for case in range(18):
            n,m,tol=rng.randint(0,7),rng.randint(0,7),rng.choice([0,1,2,4,7])
            ea=canon(('a%02d'%k,rng.randint(0,10),rng.randint(1,3)) for k in range(n))
            eb=canon(('b%02d'%k,rng.randint(0,10),rng.randint(1,3)) for k in range(m))
            da=self.make_db('a%d.db'%case,ea);db=self.make_db('b%d.db'%case,eb)
            out=self.dir/('r%d.json'%case)
            r=self.run_cli(da,db,tol,out);self.assertEqual(r.returncode,0,r.stderr)
            report=json.loads(out.read_text())
            self.check_report(report,ea,eb,tol)
            want=brute([(t,v) for _,t,v in ea],[(t,v) for _,t,v in eb],tol)
            self.assertEqual((report['objective']['pair_count'],report['objective']['total_abs_time_delta']),want)
            if case==0:
                again=self.dir/'r0b.json'
                self.assertEqual(self.run_cli(da,db,tol,again).returncode,0)
                self.assertEqual(again.read_bytes(),out.read_bytes())  # rerun reproducible

    def test_greedy_counterexample(self):
        # greedy takes a0->b1 (delta 5 < 6) and strands a1; the optimum pairs both
        ea=[('a0',10,1),('a1',25,1)];eb=[('b0',4,1),('b1',15,1)]
        ta=[(t,v) for _,t,v in ea];tb=[(t,v) for _,t,v in eb]
        self.assertEqual(greedy(ta,tb,10),(1,5))
        self.assertEqual(brute(ta,tb,10),(2,16))
        da=self.make_db('a.db',ea);db=self.make_db('b.db',eb)
        out=self.dir/'r.json'
        self.assertEqual(self.run_cli(da,db,10,out).returncode,0)
        report=json.loads(out.read_text())
        self.assertEqual(report['objective'],{'pair_count':2,'total_abs_time_delta':16})
        self.check_report(report,ea,eb,10)
        rng=random.Random(7);found=0
        for _ in range(400):  # more greedy failures in this seeded sweep; reconcile stays optimal
            n,m,tol=rng.randint(1,6),rng.randint(1,6),rng.choice([1,2,3,5])
            a=sorted((rng.randint(0,10),rng.randint(1,2)) for _ in range(n))
            b=sorted((rng.randint(0,10),rng.randint(1,2)) for _ in range(m))
            want=brute(a,b,tol)
            if greedy(a,b,tol)!=want:
                found+=1
                pairs=reconcile_mod.match(a,b,tol)
                self.assertEqual((len(pairs),sum(abs(a[i][0]-b[j][0]) for i,j in pairs)),want)
        self.assertGreaterEqual(found,3)

    def test_tie_broken_deterministically(self):
        da=self.make_db('a.db',[('a0',5,1)])  # b0 and b1 symmetric: two optima with equal objectives
        db=self.make_db('b.db',[('b0',3,1),('b1',7,1)])
        out=self.dir/'r.json'
        self.assertEqual(self.run_cli(da,db,2,out).returncode,0)
        report=json.loads(out.read_text())
        self.assertEqual(report['objective'],{'pair_count':1,'total_abs_time_delta':2})  # brute optimum
        self.assertEqual(brute([(5,1)],[(3,1),(7,1)],2),(1,2))
        self.assertEqual([(p['a_index'],p['b_index']) for p in report['pairs']],[(0,1)])  # deterministic pick
        self.assertEqual([e['id'] for e in report['unmatched_b']],['b0'])
        again=self.dir/'again.json'
        self.assertEqual(self.run_cli(da,db,2,again).returncode,0)
        self.assertEqual(again.read_bytes(),out.read_bytes())  # rerun reproducible
        dc=self.make_db('c.db',[('c0',1,1)])  # identical candidate events: tie again
        dd=self.make_db('d.db',[('d0',1,1),('d1',1,1)])
        out2=self.dir/'r2.json'
        self.assertEqual(self.run_cli(dc,dd,0,out2).returncode,0)
        report=json.loads(out2.read_text())
        self.assertEqual(report['objective'],{'pair_count':1,'total_abs_time_delta':0})
        self.assertEqual([(p['a_index'],p['b_index']) for p in report['pairs']],[(0,1)])
        self.assertEqual([e['id'] for e in report['unmatched_b']],['d0'])
        again2=self.dir/'again2.json'
        self.assertEqual(self.run_cli(dc,dd,0,again2).returncode,0)
        self.assertEqual(again2.read_bytes(),out2.read_bytes())

    def test_empty_sources(self):
        da=self.make_db('a.db',[]);db=self.make_db('b.db',[])
        out=self.dir/'r.json'
        self.assertEqual(self.run_cli(da,db,5,out).returncode,0)
        report=json.loads(out.read_text())
        empty_sha=hashlib.sha256(b'').hexdigest()
        self.assertEqual(report['objective'],{'pair_count':0,'total_abs_time_delta':0})
        self.assertEqual(report['pairs'],[]);self.assertEqual(report['unmatched_a'],[]);self.assertEqual(report['unmatched_b'],[])
        self.assertEqual(report['sources']['a']['canonical_sha256'],empty_sha)
        self.assertEqual(report['sources']['b']['canonical_sha256'],empty_sha)
        dc=self.make_db('c.db',[('x',1,1),('y',2,2),('z',3,3)])  # one side empty
        out2=self.dir/'r2.json'
        self.assertEqual(self.run_cli(dc,db,5,out2).returncode,0)
        report=json.loads(out2.read_text())
        self.assertEqual(report['objective']['pair_count'],0)
        self.assertEqual([e['id'] for e in report['unmatched_a']],['x','y','z'])
        self.assertEqual(report['unmatched_b'],[])

    def test_only_accepted_events_reconciled(self):
        db=self.dir/'a.db';stream.init(db,['p1'],10,0)
        def ev(k,eid,t,v):return {'partition':'p1','offset':k,'kind':'event','id':eid,'event_time':t,'value':v}
        def apply(records,name):
            p=self.dir/name;p.write_text(''.join(json.dumps(r)+'\n' for r in records));return stream.apply(db,p)
        apply([ev(0,'e1',100,7)],'b1.jsonl')              # accepted; watermark 100 closes [0,10)..[90,100)
        apply([ev(1,'e2',5,1),ev(2,'e3',50,2)],'b2.jsonl')  # both late
        apply([ev(3,'e1',100,7)],'b3.jsonl')              # duplicate id, same content
        r=apply([ev(0,'e1',100,7)],'b1again.jsonl')       # retransmission: no-op
        self.assertEqual((r['accepted_count'],r['duplicate_events'],len(r['late_events'])),(1,1,2))
        empty=self.make_db('empty.db',[])
        out=self.dir/'r.json'
        self.assertEqual(self.run_cli(db,empty,1000,out).returncode,0)
        report=json.loads(out.read_text())  # only the accepted e1 enters the canonical sequence
        self.assertEqual(report['sources']['a']['accepted_events'],1)
        self.assertEqual(report['unmatched_a'],[{'index':0,'id':'e1','event_time':100,'value':7}])
        self.assertEqual(report['sources']['a']['canonical_sha256'],canon_sha([('e1',100,7)]))

    def test_same_source_both_sides(self):
        da=self.make_db('a.db',[('a0',1,5),('a1',3,6),('a2',9,5)])
        out=self.dir/'r.json'
        self.assertEqual(self.run_cli(da,da,0,out).returncode,0)
        report=json.loads(out.read_text())
        self.assertEqual(report['objective'],{'pair_count':3,'total_abs_time_delta':0})
        self.assertEqual([(p['a_index'],p['b_index']) for p in report['pairs']],[(0,0),(1,1),(2,2)])

    def test_invalid_sources_and_params(self):
        da=self.make_db('a.db',[('a0',1,1)]);db=self.make_db('b.db',[('b0',2,1)])
        out=self.dir/'r.json';out.write_text('prior')
        missing=self.dir/'missing.db'
        self.assertEqual(self.run_cli(missing,db,1,out).returncode,2)
        self.assertEqual(out.read_text(),'prior')  # failure preserves the old report
        corrupt=self.dir/'corrupt.db';corrupt.write_text('not sqlite')
        self.assertEqual(self.run_cli(corrupt,db,1,out).returncode,2)
        self.assertEqual(out.read_text(),'prior')
        foreign=self.dir/'foreign.db';sqlite3.connect(foreign).close()
        r=self.run_cli(foreign,db,1,out);self.assertEqual(r.returncode,2);self.assertIn('not a stream state database',r.stderr)
        badschema=self.make_db('s.db',[('x',1,1)])
        conn=sqlite3.connect(badschema);conn.execute("UPDATE meta SET value='2' WHERE key='schema'");conn.commit();conn.close()
        r=self.run_cli(badschema,db,1,out);self.assertEqual(r.returncode,2);self.assertIn('unsupported state schema',r.stderr)
        for tol in ['-1','x','1.5',str(10**12+1)]:
            self.assertEqual(self.run_cli(da,db,tol,out).returncode,2,tol)
            self.assertEqual(out.read_text(),'prior')
        r=subprocess.run([sys.executable,'-m','watermark_windows.cli','reconcile','--a',str(da),'--b',str(db),
                          '--output',str(out)],capture_output=True,text=True)
        self.assertEqual(r.returncode,2)  # missing required --tolerance
        self.assertEqual(self.run_cli(da,db,1,da).returncode,2)  # output would overwrite source a
        self.assertEqual(self.run_cli(da,db,1,db).returncode,2)  # output would overwrite source b
        link=self.dir/'link.json';link.symlink_to('nowhere')
        self.assertEqual(self.run_cli(da,db,1,link).returncode,2)
        self.assertEqual(out.read_text(),'prior')
        ha=hashlib.sha256(da.read_bytes()).hexdigest();hb=hashlib.sha256(db.read_bytes()).hexdigest()
        self.assertEqual(self.run_cli(da,db,10**12,out).returncode,0)  # boundary tolerance accepted
        self.assertEqual(hashlib.sha256(da.read_bytes()).hexdigest(),ha)  # sources stay read-only
        self.assertEqual(hashlib.sha256(db.read_bytes()).hexdigest(),hb)

    def test_concurrent_apply_snapshot_not_torn(self):
        n=2500
        ea=[('a%04d'%i,i,1) for i in range(n)];eb=[('b%04d'%i,i,1) for i in range(n)]
        da=self.make_db('a.db',ea);db=self.make_db('b.db',eb)
        out1=self.dir/'r1.json'
        r=self.run_cli(da,db,3000,out1);self.assertEqual(r.returncode,0,r.stderr)
        before=out1.read_bytes()
        self.assertEqual(json.loads(before)['objective'],{'pair_count':n,'total_abs_time_delta':0})
        extra=self.dir/'extra.jsonl'
        extra.write_text(json.dumps({'partition':'p1','offset':n,'kind':'event','id':'zz_extra',
                                     'event_time':1500,'value':1})+'\n')
        applied={}
        def writer():
            time.sleep(0.9)  # reconcile has taken its snapshot by now and is crunching the DP
            applied['report']=stream.apply(da,extra)
            applied['done']=time.monotonic()
        th=threading.Thread(target=writer);th.start()
        out2=self.dir/'r2.json'
        r2=self.run_cli(da,db,3000,out2);ended=time.monotonic()
        th.join()
        self.assertEqual(r2.returncode,0,r2.stderr)
        self.assertEqual(out2.read_bytes(),before)  # snapshot preceded the concurrent commit: no tear
        self.assertIn('done',applied)
        self.assertLess(applied['done'],ended)  # apply really committed while reconcile was running
        self.assertEqual(applied['report']['accepted_count'],n+1)
        self.assertEqual(stream.report(da)['accepted_count'],n+1)

    def test_6000x6000_full_detail_and_peak_memory(self):
        n=6000  # values make only (i,i) compatible: the optimum is forced, 6000 pairs of delta 1
        ea=[('a%04d'%i,2*i,(i%7)+1) for i in range(n)]
        eb=[('b%04d'%i,2*i+1,(i%7)+1) for i in range(n)]
        da=self.make_db('a.db',ea);db=self.make_db('b.db',eb)
        out=self.dir/'r.json'
        proc=subprocess.Popen([sys.executable,'-m','watermark_windows.cli','reconcile','--a',str(da),
                               '--b',str(db),'--tolerance','1','--output',str(out)],
                              stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        peak=[0];stop=threading.Event()
        def poll():
            path='/proc/%d/status'%proc.pid
            while not stop.is_set():
                try:
                    with open(path) as fh:
                        for line in fh:
                            if line.startswith('VmHWM:'):peak[0]=max(peak[0],int(line.split()[1]))
                except OSError:pass
                stop.wait(0.02)
        th=threading.Thread(target=poll);th.start()
        started=time.monotonic();stdout,stderr=proc.communicate();duration=time.monotonic()-started
        stop.set();th.join()
        self.assertEqual(proc.returncode,0,stderr)
        if peak[0]==0:self.skipTest('/proc not available for peak RSS')
        report=json.loads(out.read_text())
        self.assertEqual(report['objective'],{'pair_count':n,'total_abs_time_delta':n})
        pairs=report['pairs'];self.assertEqual(len(pairs),n)  # full detail reconstructed
        for k,p in enumerate(pairs):
            self.assertEqual((p['a_index'],p['b_index'],p['time_delta']),(k,k,1))
        self.assertEqual(report['unmatched_a'],[]);self.assertEqual(report['unmatched_b'],[])
        self.assertEqual(report['sources']['a']['canonical_sha256'],canon_sha(ea))
        self.assertEqual(report['sources']['b']['canonical_sha256'],canon_sha(eb))
        # O(min(n,m)) workspace: an n-by-m table of Python ints alone would exceed 1 GB
        self.assertLess(peak[0],300*1024,'peak RSS %d kB suggests a quadratic table'%peak[0])
        self.assertLess(duration,240)

class Matching(unittest.TestCase):
    def test_against_brute_force(self):
        rng=random.Random(20261115)
        for _ in range(500):
            n,m,tol=rng.randint(0,8),rng.randint(0,8),rng.choice([0,1,2,3,5,8])
            a=sorted((rng.randint(0,12),rng.randint(1,3)) for _ in range(n))
            b=sorted((rng.randint(0,12),rng.randint(1,3)) for _ in range(m))
            pairs=reconcile_mod.match(a,b,tol)
            for i,j in pairs:
                self.assertEqual(a[i][1],b[j][1]);self.assertLessEqual(abs(a[i][0]-b[j][0]),tol)
            self.assertTrue(all(pairs[k][0]<pairs[k+1][0] and pairs[k][1]<pairs[k+1][1]
                                for k in range(len(pairs)-1)))
            got=(len(pairs),sum(abs(a[i][0]-b[j][0]) for i,j in pairs))
            self.assertEqual(got,brute(a,b,tol))

if __name__=='__main__':unittest.main()
