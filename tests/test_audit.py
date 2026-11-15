
import hashlib,json,random,sqlite3,subprocess,sys,tempfile,time,unittest
from fractions import Fraction
from pathlib import Path
from watermark_windows import stream
from watermark_windows import audit as audit_mod

def make_db(directory,name,events,width=86400,delay=0):
    db=directory/name
    stream.init(db,['p1'],width,delay)
    if events:
        batch=directory/(name+'.jsonl')
        batch.write_text(''.join(json.dumps({'partition':'p1','offset':k,'kind':'event','id':e[0],
                                             'event_time':e[1],'value':e[2]})+'\n'
                                 for k,e in enumerate(events)))
        stream.apply(db,batch)
    return db

def canon(events):
    return sorted(events,key=lambda e:(e[1],e[0]))

# --- independent exact linear algebra: nothing below reuses audit code ---

def rank(rows,ncols):
    basis=[]
    for r in rows:
        v=[Fraction(x) for x in r]
        for pc,prow in basis:
            if v[pc]:v=[x-v[pc]*y for x,y in zip(v,prow)]
        pc=next((k for k in range(ncols) if v[k]),None)
        if pc is not None:
            inv=1/v[pc];basis.append((pc,[x*inv for x in v]))
    return len(basis)

def identifiable(kept,ncols,i):
    unit=[1 if k==i else 0 for k in range(ncols)]
    return rank(kept,ncols)==rank(kept+[unit],ncols)

def brute(incidence,costs,qids,protected_idx,ncols):
    # exhaustive global optimum: min total suppressed cost, then lexicographically
    # smallest ascending query_id list
    best=None
    for mask in range(1<<len(incidence)):
        kept=[incidence[q] for q in range(len(incidence)) if not (mask>>q)&1]
        if all(not identifiable(kept,ncols,i) for i in protected_idx):
            cost=sum(costs[q] for q in range(len(incidence)) if (mask>>q)&1)
            key=sorted(qids[q] for q in range(len(incidence)) if (mask>>q)&1)
            if best is None or (cost,key)<best:best=(cost,key)
    return best

def greedy(incidence,costs,qids,protected_idx,ncols):
    # keep-expensive-first heuristic; not always optimal (see test_greedy_not_optimal)
    kept_rows=[];kept_idx=set()
    for q in sorted(range(len(incidence)),key=lambda q:(-costs[q],qids[q])):
        if all(not identifiable(kept_rows+[incidence[q]],ncols,i) for i in protected_idx):
            kept_rows.append(incidence[q]);kept_idx.add(q)
    return sum(costs[q] for q in range(len(incidence)) if q not in kept_idx)

def frac(s):
    p,q=s.split('/');return Fraction(int(p),int(q))

def cert_vector(cert,ids):
    d=[Fraction(0)]*len(ids)
    for k,v in cert.items():d[ids.index(k)]=frac(v)
    return d

def canon_sha(events,queries,protected):
    # pins the documented encoding in docs/audit.md
    digest=hashlib.sha256()
    def line(obj):
        digest.update(json.dumps(obj,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode('utf-8'))
        digest.update(b'\n')
    for eid,moment,value in events:line({'event_time':moment,'id':eid,'value':value})
    for qid,cost,members in sorted(queries,key=lambda q:q[0]):
        line({'cost':cost,'event_ids':sorted(members),'query_id':qid})
    line({'protected_ids':sorted(protected)})
    return digest.hexdigest()

class AuditCLI(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.dir=Path(self.tmp.name)
    def plan_path(self,obj,name='plan.json'):
        p=self.dir/name;p.write_text(json.dumps(obj));return p
    def run_cli(self,db,plan,out):
        return subprocess.run([sys.executable,'-m','watermark_windows.cli','audit',
                               '--db',str(db),'--plan',str(plan),'--output',str(out)],
                              capture_output=True,text=True)
    def structures(self,events,queries):
        ids=[e[0] for e in canon(events)]
        values={e[0]:e[2] for e in events}
        rows={q['query_id']:[1 if eid in set(q['event_ids']) else 0 for eid in ids] for q in queries}
        return ids,values,rows
    def check_kernel_certificate(self,cert,matrix,ids,eid):
        d=cert_vector(cert,ids)
        self.assertEqual(d[ids.index(eid)],1)  # d_i = 1
        for row in matrix:
            self.assertEqual(sum(Fraction(x)*y for x,y in zip(row,d)),0)  # Bd = 0
    def verify_report(self,report,events,queries,protected):
        ids,values,qrows=self.structures(events,queries)
        self.assertEqual(report['schema_version'],1)
        self.assertEqual(report['input_sha256'],
                         canon_sha(canon(events),[(q['query_id'],q['cost'],q['event_ids']) for q in queries],protected))
        self.assertEqual(report['publication']['event_ids'],ids)
        suppressed={q['query_id'] for q in report['suppression']['suppressed_queries']}
        kept={q['query_id'] for q in report['publication']['kept_queries']}
        self.assertEqual(suppressed|kept,{q['query_id'] for q in queries})
        self.assertEqual(suppressed&kept,set())
        self.assertEqual(report['suppression']['total_cost'],
                         sum(q['cost'] for q in queries if q['query_id'] in suppressed))
        for entry in report['publication']['kept_queries']:
            src=next(q for q in queries if q['query_id']==entry['query_id'])
            self.assertEqual(entry['event_ids'],src['event_ids'])
            self.assertEqual(entry['cost'],src['cost'])
            self.assertEqual(entry['sum'],sum(values[e] for e in src['event_ids']))
        for entry in report['suppression']['suppressed_queries']:
            self.assertNotIn('sum',entry)  # suppressed query sums stay sealed
        qid_order=[q['query_id'] for q in sorted(queries,key=lambda q:q['query_id'])]
        per={e['event_id']:e for e in report['original_plan']['per_event']}
        self.assertEqual(set(per),set(protected))
        for eid,entry in per.items():
            if entry['identifiable']:
                c=cert_vector(entry['coefficients'],qid_order)
                combo=[sum(c[j]*qrows[qid_order[j]][k] for j in range(len(qid_order))) for k in range(len(ids))]
                self.assertEqual(combo,[1 if ids[k]==eid else 0 for k in range(len(ids))])  # cA = e_i
                self.assertEqual(entry['derived_value'],values[eid])
            else:
                self.check_kernel_certificate(entry['kernel_certificate'],list(qrows.values()),ids,eid)
        kept_rows=[qrows[qid] for qid in qid_order if qid in kept]
        per_kept={e['event_id']:e for e in report['kept_plan']['per_event']}
        self.assertEqual(set(per_kept),set(protected))
        for eid,entry in per_kept.items():
            self.check_kernel_certificate(entry['kernel_certificate'],kept_rows,ids,eid)
        return suppressed

    def test_joint_leakage_via_combination(self):
        # no single query is e1, but e1 = q1 - q2: a joint combination leaks it
        events=[('e1',1,7),('e2',2,3)]
        queries=[{'query_id':'q1','cost':5,'event_ids':['e1','e2']},
                 {'query_id':'q2','cost':3,'event_ids':['e2']}]
        db=make_db(self.dir,'a.db',events)
        plan=self.plan_path({'queries':queries,'protected_ids':['e1']})
        out=self.dir/'r.json'
        self.assertEqual(self.run_cli(db,plan,out).returncode,0)
        report=json.loads(out.read_text())
        per=report['original_plan']['per_event'][0]
        self.assertEqual(per,{'event_id':'e1','identifiable':True,
                              'coefficients':{'q1':'1/1','q2':'-1/1'},'derived_value':7})
        self.assertEqual(report['suppression']['total_cost'],3)  # suppress q2, cheaper than q1
        self.assertEqual([q['query_id'] for q in report['suppression']['suppressed_queries']],['q2'])
        self.assertEqual(report['kept_plan']['per_event'][0]['kernel_certificate'],{'e1':'1/1','e2':'-1/1'})
        self.verify_report(report,events,queries,['e1'])
        again=self.dir/'again.json'
        self.assertEqual(self.run_cli(db,plan,again).returncode,0)
        self.assertEqual(again.read_bytes(),out.read_bytes())  # rerun byte-identical

    def test_joint_sum_leakage_is_not_identification(self):
        # q1 publishes e1+e2: the joint sum leaks, yet neither event is individually
        # identifiable, so the per-event criterion suppresses nothing
        events=[('e1',1,7),('e2',2,3)]
        queries=[{'query_id':'q1','cost':5,'event_ids':['e1','e2']}]
        db=make_db(self.dir,'a.db',events)
        plan=self.plan_path({'queries':queries,'protected_ids':['e1','e2']})
        out=self.dir/'r.json'
        self.assertEqual(self.run_cli(db,plan,out).returncode,0)
        report=json.loads(out.read_text())
        self.assertEqual(report['suppression']['total_cost'],0)
        self.assertEqual(report['suppression']['suppressed_queries'],[])
        per={e['event_id']:e for e in report['original_plan']['per_event']}
        self.assertEqual(per['e1']['identifiable'],False)
        self.assertEqual(per['e2']['identifiable'],False)
        self.assertEqual(report['kept_plan']['per_event'][0]['kernel_certificate'],{'e1':'1/1','e2':'-1/1'})
        self.verify_report(report,events,queries,['e1','e2'])

    def test_rank_deficiency(self):
        # q1 and q2 are the same row: rank-deficient plan, certificates stay exact
        events=[('e1',1,4),('e2',2,2)]
        queries=[{'query_id':'q1','cost':1,'event_ids':['e1','e2']},
                 {'query_id':'q2','cost':1,'event_ids':['e1','e2']},
                 {'query_id':'q3','cost':3,'event_ids':['e2']}]
        db=make_db(self.dir,'a.db',events)
        plan=self.plan_path({'queries':queries,'protected_ids':['e1']})
        out=self.dir/'r.json'
        self.assertEqual(self.run_cli(db,plan,out).returncode,0)
        report=json.loads(out.read_text())
        per=report['original_plan']['per_event'][0]
        self.assertEqual(per['identifiable'],True)  # e1 = q1 - q3 = q2 - q3
        self.assertEqual(per['derived_value'],4)
        # hitting every certificate: {q1,q2} costs 2 and beats {q3} at 3
        self.assertEqual(report['suppression']['total_cost'],2)
        self.assertEqual([q['query_id'] for q in report['suppression']['suppressed_queries']],['q1','q2'])
        self.assertEqual(report['kept_plan']['per_event'][0]['kernel_certificate'],{'e1':'1/1'})
        self.verify_report(report,events,queries,['e1'])

    def test_greedy_not_optimal(self):
        events=[('e1',1,1),('e2',2,2),('e3',3,3)]
        queries=[{'query_id':'q1','cost':7,'event_ids':['e1','e2']},
                 {'query_id':'q2','cost':6,'event_ids':['e3']},
                 {'query_id':'q3','cost':9,'event_ids':['e1','e3']},
                 {'query_id':'q4','cost':8,'event_ids':['e2','e3']},
                 {'query_id':'q5','cost':5,'event_ids':['e1','e2','e3']}]
        protected=['e1','e2']
        ids,values,qrows=self.structures(events,queries)
        idx={eid:k for k,eid in enumerate(ids)}
        qid_order=[q['query_id'] for q in sorted(queries,key=lambda q:q['query_id'])]
        incidence=[qrows[q] for q in qid_order];costs=[q['cost'] for q in sorted(queries,key=lambda q:q['query_id'])]
        pidx=[idx[e] for e in protected]
        self.assertEqual(greedy(incidence,costs,qid_order,pidx,len(ids)),18)  # keeps {q3,q4}
        self.assertEqual(brute(incidence,costs,qid_order,pidx,len(ids)),(17,['q3','q4']))
        db=make_db(self.dir,'a.db',events)
        plan=self.plan_path({'queries':queries,'protected_ids':protected})
        out=self.dir/'r.json'
        self.assertEqual(self.run_cli(db,plan,out).returncode,0)
        report=json.loads(out.read_text())
        self.assertEqual(report['suppression']['total_cost'],17)  # keeps {q1,q2,q5}
        self.assertEqual([q['query_id'] for q in report['suppression']['suppressed_queries']],['q3','q4'])
        self.verify_report(report,events,queries,protected)

    def test_tie_lexicographic(self):
        # four optima at cost 2: {q1,q3},{q1,q4},{q2,q3},{q2,q4}; lexicographically
        # smallest ascending query_id list wins
        events=[('e1',1,1),('e2',2,1),('e3',3,1),('e4',4,1)]
        queries=[{'query_id':'q1','cost':1,'event_ids':['e1','e2']},
                 {'query_id':'q2','cost':1,'event_ids':['e2']},
                 {'query_id':'q3','cost':1,'event_ids':['e3','e4']},
                 {'query_id':'q4','cost':1,'event_ids':['e4']}]
        db=make_db(self.dir,'a.db',events)
        plan=self.plan_path({'queries':queries,'protected_ids':['e1','e3']})
        out=self.dir/'r.json'
        self.assertEqual(self.run_cli(db,plan,out).returncode,0)
        report=json.loads(out.read_text())
        self.assertEqual(report['suppression']['total_cost'],2)
        self.assertEqual([q['query_id'] for q in report['suppression']['suppressed_queries']],['q1','q3'])
        self.verify_report(report,events,queries,['e1','e3'])
        again=self.dir/'again.json'
        self.assertEqual(self.run_cli(db,plan,again).returncode,0)
        self.assertEqual(again.read_bytes(),out.read_bytes())

    def test_empty_plan(self):
        db=make_db(self.dir,'a.db',[('e1',1,7),('e2',2,3)])
        out=self.dir/'r.json'
        self.assertEqual(self.run_cli(db,self.plan_path({'queries':[],'protected_ids':[]}),out).returncode,0)
        report=json.loads(out.read_text())
        self.assertEqual(report['publication']['event_ids'],['e1','e2'])
        self.assertEqual(report['publication']['kept_queries'],[])
        self.assertEqual(report['suppression'],{'total_cost':0,'suppressed_queries':[]})
        self.assertEqual(report['original_plan']['per_event'],[])
        self.assertEqual(report['kept_plan']['per_event'],[])
        # queries but nothing protected: keep everything, sums published
        queries=[{'query_id':'q1','cost':5,'event_ids':['e1']},{'query_id':'q2','cost':1,'event_ids':['e2']}]
        out2=self.dir/'r2.json'
        self.assertEqual(self.run_cli(db,self.plan_path({'queries':queries,'protected_ids':[]},'p2.json'),out2).returncode,0)
        report=json.loads(out2.read_text())
        self.assertEqual(report['suppression']['total_cost'],0)
        self.assertEqual([(q['query_id'],q['sum']) for q in report['publication']['kept_queries']],[('q1',7),('q2',3)])
        # empty database plus empty plan
        empty=make_db(self.dir,'empty.db',[])
        out3=self.dir/'r3.json'
        self.assertEqual(self.run_cli(empty,self.plan_path({'queries':[],'protected_ids':[]},'p3.json'),out3).returncode,0)
        report=json.loads(out3.read_text())
        self.assertEqual(report['publication']['event_ids'],[])
        self.assertEqual(report['input_sha256'],canon_sha([],[],[]))

    def test_exhaustive_small(self):
        rng=random.Random(20261115)
        for case in range(150):
            n,m=rng.randint(0,8),rng.randint(0,6)
            events=[('e%d'%k,k,rng.randint(-5,9)) for k in range(n)]
            queries=[]
            for q in range(m):
                members=[e[0] for e in events if rng.random()<0.5]
                rng.shuffle(members)
                queries.append({'query_id':'q%d'%q,'cost':rng.randint(1,5),'event_ids':members})
            protected=[e[0] for e in events if rng.random()<0.4]
            rng.shuffle(protected)
            db=make_db(self.dir,'c%d.db'%case,events)
            plan=self.plan_path({'queries':queries,'protected_ids':protected},'p%d.json'%case)
            report=audit_mod.audit(db,plan)
            self.verify_report(report,events,queries,protected)  # all certificates checked independently
            ids,values,qrows=self.structures(events,queries)
            idx={eid:k for k,eid in enumerate(ids)}
            ordered=sorted(queries,key=lambda q:q['query_id'])
            want=brute([qrows[q['query_id']] for q in ordered],[q['cost'] for q in ordered],
                       [q['query_id'] for q in ordered],[idx[e] for e in protected],len(ids))
            got=(report['suppression']['total_cost'],
                 sorted(q['query_id'] for q in report['suppression']['suppressed_queries']))
            self.assertEqual(got,want)  # global optimum, tie broken as specified
            self.assertEqual(audit_mod.audit(db,plan),report)  # deterministic rerun

    def test_limits_80_events_22_queries(self):
        # 11 disjoint pairs: protecting a_k = q(2k) - q(2k+1) forces one suppression
        # per pair; 2^11 tied optima at cost 11, lexicographically smallest wins
        events=[]
        for k in range(11):
            events.append(('a%02d'%k,2*k,100+k));events.append(('b%02d'%k,2*k+1,7))
        events += [('f%02d'%f,22+f,f) for f in range(58)]
        self.assertEqual(len(events),80)
        queries=[]
        for k in range(11):
            queries.append({'query_id':'q%02d'%(2*k),'cost':1,'event_ids':['a%02d'%k,'b%02d'%k]})
            queries.append({'query_id':'q%02d'%(2*k+1),'cost':1,'event_ids':['b%02d'%k]})
        protected=['a%02d'%k for k in range(11)]
        db=make_db(self.dir,'big.db',events)
        plan=self.plan_path({'queries':queries,'protected_ids':protected})
        out=self.dir/'r.json'
        started=time.monotonic()
        r=self.run_cli(db,plan,out)
        duration=time.monotonic()-started
        self.assertEqual(r.returncode,0,r.stderr)
        report=json.loads(out.read_text())
        self.assertEqual(len(report['publication']['event_ids']),80)
        self.assertEqual(report['suppression']['total_cost'],11)
        self.assertEqual([q['query_id'] for q in report['suppression']['suppressed_queries']],
                         ['q%02d'%(2*k) for k in range(11)])
        self.verify_report(report,events,queries,protected)
        print('audit 80 events / 22 queries / 11 protected: %.2fs' % duration)
        self.assertLess(duration,120)

    def test_only_accepted_events(self):
        db=self.dir/'a.db';stream.init(db,['p1'],10,0)
        def ev(k,eid,t,v):return {'partition':'p1','offset':k,'kind':'event','id':eid,'event_time':t,'value':v}
        def apply(records,name):
            p=self.dir/name;p.write_text(''.join(json.dumps(r)+'\n' for r in records));return stream.apply(db,p)
        apply([ev(0,'e1',5,7),ev(1,'e2',50,2)],'b1.jsonl')   # both accepted; watermark 50
        apply([ev(2,'L',3,1)],'b2.jsonl')                    # late: window [0,10) already closed
        apply([ev(3,'e1',5,7)],'b3.jsonl')                   # duplicate id, same content
        r=stream.report(db)
        self.assertEqual((r['accepted_count'],r['duplicate_events'],len(r['late_events'])),(2,1,1))
        plan=self.plan_path({'queries':[{'query_id':'q1','cost':1,'event_ids':['e1','e2']}],'protected_ids':['e1']})
        out=self.dir/'r.json'
        self.assertEqual(self.run_cli(db,plan,out).returncode,0)
        report=json.loads(out.read_text())
        self.assertEqual(report['publication']['event_ids'],['e1','e2'])  # late and duplicate excluded
        late=self.plan_path({'queries':[{'query_id':'q1','cost':1,'event_ids':['L']}],'protected_ids':[]},'late.json')
        r=self.run_cli(db,late,out)
        self.assertEqual(r.returncode,2);self.assertIn('late event',r.stderr)
        late2=self.plan_path({'queries':[],'protected_ids':['L']},'late2.json')
        r=self.run_cli(db,late2,out)
        self.assertEqual(r.returncode,2);self.assertIn('late event',r.stderr)
        ghost=self.plan_path({'queries':[{'query_id':'q1','cost':1,'event_ids':['ghost']}],'protected_ids':[]},'ghost.json')
        r=self.run_cli(db,ghost,out)
        self.assertEqual(r.returncode,2);self.assertIn('unknown event',r.stderr)
        ghost2=self.plan_path({'queries':[],'protected_ids':['ghost']},'ghost2.json')
        r=self.run_cli(db,ghost2,out)
        self.assertEqual(r.returncode,2);self.assertIn('unknown event',r.stderr)

    def test_invalid_inputs(self):
        db=make_db(self.dir,'a.db',[('e1',1,7),('e2',2,3)])
        good=self.plan_path({'queries':[{'query_id':'q1','cost':1,'event_ids':['e1']}],'protected_ids':['e1']},'good.json')
        out=self.dir/'r.json';out.write_text('prior')
        def fails(plan=None,database=None,output=None,fragment=None):
            r=self.run_cli(database or db,plan or good,output or out)
            self.assertEqual(r.returncode,2)
            if fragment:self.assertIn(fragment,r.stderr)
            self.assertEqual(out.read_text(),'prior')  # failure preserves the old report
        fails(database=self.dir/'missing.db',fragment='state not initialized')
        corrupt=self.dir/'corrupt.db';corrupt.write_text('not sqlite')
        fails(database=corrupt,fragment='not a stream state database')
        foreign=self.dir/'foreign.db';sqlite3.connect(foreign).close()
        fails(database=foreign,fragment='not a stream state database')
        fails(plan=self.dir/'missing.json')
        raw=self.dir/'raw.json';raw.write_text('{"queries": [], "queries": []}')
        fails(plan=raw)  # duplicate JSON key rejected
        raw.write_text('not json');fails(plan=raw)
        def bad(obj,fragment,name):
            fails(plan=self.plan_path(obj,name),fragment=fragment)
        bad({'queries':[],'protected_ids':[],'x':1},None,'f1.json')
        bad({'queries':{},'protected_ids':[]},None,'f2.json')
        bad({'queries':[{'query_id':'q%d'%k,'cost':1,'event_ids':[]} for k in range(23)],'protected_ids':[]},
            'at most 22','f3.json')
        bad({'queries':[{'query_id':'q1','cost':1,'event_ids':[]},{'query_id':'q1','cost':1,'event_ids':[]}],
             'protected_ids':[]},'duplicate query_id','f4.json')
        for cost in [0,-1,1.5,'x',10**9+1]:
            bad({'queries':[{'query_id':'q1','cost':cost,'event_ids':[]}],'protected_ids':[]},None,'f5.json')
        bad({'queries':[{'query_id':'q1','cost':1,'event_ids':['e1','e1']}],'protected_ids':[]},
            'duplicate event id','f6.json')
        bad({'queries':[{'query_id':'q1','cost':1,'event_ids':['ghost']}],'protected_ids':[]},
            'unknown event','f7.json')
        bad({'queries':[],'protected_ids':['e1','e1']},'duplicate protected','f8.json')
        bad({'queries':[],'protected_ids':['ghost']},'unknown event','f9.json')
        bad({'queries':[{'query_id':'q1','cost':1,'event_ids':[],'sum':3}],'protected_ids':[]},None,'f10.json')
        fails(output=db,fragment='overwrite input')          # output must not overwrite the state db
        fails(output=good,fragment='overwrite input')        # nor the plan
        link=self.dir/'link.json';link.symlink_to('nowhere')
        fails(output=link)
        big=make_db(self.dir,'big.db',[('e%02d'%k,k,1) for k in range(81)])
        fails(database=big,fragment='at most 80')
        # a valid run succeeds afterwards and the state db is untouched
        before=hashlib.sha256(db.read_bytes()).hexdigest()
        self.assertEqual(self.run_cli(db,good,out).returncode,0)
        self.assertEqual(hashlib.sha256(db.read_bytes()).hexdigest(),before)
        report=json.loads(out.read_text())
        self.assertEqual(report['suppression']['total_cost'],1)  # q1 = {e1} alone identifies e1
        self.assertEqual(report['original_plan']['per_event'][0]['identifiable'],True)

    def test_cli_stdout_matches_file(self):
        events=[('e1',1,7),('e2',2,3)]
        queries=[{'query_id':'q1','cost':5,'event_ids':['e1','e2']},{'query_id':'q2','cost':3,'event_ids':['e2']}]
        db=make_db(self.dir,'a.db',events)
        plan=self.plan_path({'queries':queries,'protected_ids':['e1']})
        out=self.dir/'r.json'
        r=self.run_cli(db,plan,out)
        self.assertEqual(r.returncode,0,r.stderr)
        self.assertEqual(json.loads(r.stdout),json.loads(out.read_text()))

if __name__=='__main__':unittest.main()
