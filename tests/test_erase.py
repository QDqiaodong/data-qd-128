
import hashlib,json,os,sqlite3,subprocess,sys,tempfile,time,unittest
from pathlib import Path
from watermark_windows import stream, reconcile

def cli(*args,env=None):
    run_env=dict(os.environ if env is None else env)
    return subprocess.run([sys.executable,'-m','watermark_windows.cli',*(str(a) for a in args)],
                          capture_output=True,text=True,env=run_env)

def gated(*args,gate,block=None):
    env=dict(os.environ);env['WATERMARK_GATE_DIR']=str(gate)
    if block is not None:env['WATERMARK_GATE_BLOCK']=block
    return subprocess.Popen([sys.executable,'-m','watermark_windows.cli',*(str(a) for a in args)],
                            stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,env=env)

def wait_for(gate,name,timeout=30):
    target=Path(gate)/(name+'.reached');deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        if target.exists():return
        time.sleep(0.01)
    raise AssertionError('gate never reached: '+name)

def release(gate,name):
    (Path(gate)/(name+'.release')).write_text('1')

def finish(proc,timeout=60):
    out,err=proc.communicate(timeout=timeout)
    return proc.returncode,out,err

def event(partition,offset,eid,moment,value):
    return {'partition':partition,'offset':offset,'kind':'event','id':eid,'event_time':moment,'value':value}

class EraseCase(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.dir=Path(self.tmp.name);self.db=self.dir/'state.db';self.n=0
        self.gates=[]
    def tearDown(self):
        for gate in self.gates:
            for name in ('snapshot-taken','validated','saved'):
                release(gate,name)  # never leave a gated child blocked
    def init(self,partitions='p1,p2',width=10,delay=0,db=None):
        r=cli('stream','init','--db',db or self.db,'--partitions',partitions,
              '--window-seconds',width,'--out-of-order-seconds',delay)
        self.assertEqual(r.returncode,0,r.stderr)
    def write_batch(self,records,name=None):
        self.n+=1;path=self.dir/(name or 'batch%d.jsonl'%self.n)
        path.write_text(''.join(json.dumps(r)+'\n' for r in records));return path
    def apply(self,records,db=None):
        batch=records if isinstance(records,Path) else self.write_batch(records)
        return cli('stream','apply','--db',db or self.db,'--batch',batch,'--output',self.dir/'r.json')
    def apply_ok(self,records,db=None):
        out=self.dir/('r%d.json'%self.n)
        batch=records if isinstance(records,Path) else self.write_batch(records)
        r=cli('stream','apply','--db',db or self.db,'--batch',batch,'--output',out)
        self.assertEqual(r.returncode,0,r.stderr)
        return json.loads(out.read_text())
    def ids_file(self,ids,name='ids.json'):
        p=self.dir/name;p.write_text(json.dumps({'event_ids':ids}));return p
    def erase(self,ids,out=None,db=None):
        return cli('stream','erase','--db',db or self.db,'--ids',self.ids_file(ids),
                   '--output',out or self.dir/'e.json')
    def erase_ok(self,ids,db=None):
        out=self.dir/('e%d.json'%self.n);self.n+=1
        r=self.erase(ids,out,db);self.assertEqual(r.returncode,0,r.stderr)
        return json.loads(out.read_text())
    def report(self,db=None):
        out=self.dir/('rep%d.json'%self.n);self.n+=1
        r=cli('stream','report','--db',db or self.db,'--output',out)
        self.assertEqual(r.returncode,0,r.stderr);return json.loads(out.read_text())
    def gate_dir(self):
        gate=self.dir/('gate%d'%len(self.gates));gate.mkdir();self.gates.append(gate);return gate

class EraseBasics(EraseCase):
    def test_erase_deducts_payload_and_keeps_ledger(self):
        self.init()  # p1,p2 width10 delay0
        r=self.apply_ok([event('p1',0,'A',1,2),event('p2',0,'X',3,1),event('p1',1,'B',12,3)])
        self.assertEqual(r['accepted_count'],3)
        before=r['partitions']['p1'];watermark=r['watermark']
        out=self.dir/'erased.json'
        self.assertEqual(self.erase(['A'],out).returncode,0)
        r=json.loads(out.read_text())
        self.assertEqual(r['erase_epoch'],1)
        self.assertEqual(r['erased_event_ids'],['A'])
        self.assertEqual((r['accepted_count'],r['accepted_sum']),(2,4))
        self.assertEqual(r['pending_windows'][0]['event_ids'],['X'])  # A's window keeps X only
        self.assertEqual(r['partitions']['p1'],before)  # offsets and watermark-at-rest unchanged
        self.assertEqual(r['watermark'],watermark)
        # report rebuilds byte-identical output from state alone
        again=self.dir/'again.json';self.assertEqual(cli('stream','report','--db',self.db,'--output',again).returncode,0)
        self.assertEqual(again.read_bytes(),out.read_bytes())
        idem=self.dir/'idem.json'
        self.assertEqual(self.erase(['A'],idem).returncode,0)  # idempotent: no epoch advance
        self.assertEqual(json.loads(idem.read_text())['erase_epoch'],1)
        self.assertEqual(idem.read_bytes(),out.read_bytes())
        # the event id is not resurrected by a payload-free retransmission of its old record,
        # and a brand new offset carrying the erased id is rejected as a whole batch
        self.assertEqual(self.apply(self.dir/'batch1.jsonl').returncode,0)  # full old batch: all no-ops
        bad=self.write_batch([event('p2',1,'Y',22,1),event('p2',2,'A',1,2)])
        self.assertEqual(self.apply(bad).returncode,2)
        r=self.report()
        self.assertEqual((r['accepted_count'],r['partitions']['p2']['next_offset']),(2,1))  # Y never applied

    def test_idempotent_and_unknown_rolls_back(self):
        self.init(delay=0)
        self.apply_ok([event('p1',0,'A',1,2),event('p1',1,'B',12,3)])
        first=self.erase_ok(['A'])
        self.assertEqual(first['erase_epoch'],1)
        again=self.erase_ok(['A'])  # repeated erase: idempotent, no epoch advance
        self.assertEqual(again['erase_epoch'],1)
        self.assertEqual(again['erased_event_ids'],['A'])
        self.assertEqual(again['closed_windows'],first['closed_windows'])
        # unknown id in a batch that otherwise erases a real event: whole erase rolls back,
        # including the payloads already cleared for B before ghost was reached
        body_before=self._body('p1',1)
        self.assertIsNotNone(body_before)
        before=self.report()
        self.assertEqual(self.erase(['B','ghost']).returncode,2)
        after=self.report()
        self.assertEqual(after,before)
        self.assertEqual(after['erase_epoch'],1)
        self.assertEqual(after['erased_event_ids'],['A'])
        self.assertEqual(after['accepted_count'],1)
        self.assertEqual(self._body('p1',1),body_before)  # B's record payload survived
        self.assertEqual(self.erase_ok(['B'])['erase_epoch'],2)  # state unchanged, B still present

    def _body(self,partition,offset):
        conn=sqlite3.connect(self.db)
        try:
            row=conn.execute('SELECT body FROM records WHERE partition=? AND offset=?',(partition,offset)).fetchone()
            return None if row is None else row[0]
        finally:conn.close()

    def test_closed_window_stays_closed_and_empty_window_kept(self):
        self.init(delay=0)
        r=self.apply_ok([event('p1',0,'A',1,2),event('p2',0,'X',3,1),event('p1',1,'B',30,5),event('p2',1,'Y',31,1)])
        self.assertEqual([w['start'] for w in r['closed_windows']],[0])
        r=self.erase_ok(['A','X'])
        self.assertEqual([(w['start'],w['count'],w['sum'],w['event_ids'],w['closed_by']) for w in r['closed_windows']],
                         [(0,0,0,[],'watermark')])  # window [0,10) stays closed, empty but retained
        self.assertEqual(r['watermark'],30)  # watermark never retreats
        self.assertEqual([(w['start'],w['event_ids']) for w in r['pending_windows']],[(30,['B','Y'])])
        self.assertEqual(r['erased_event_ids'],['A','X'])

    def test_cross_partition_duplicate_and_late_ids(self):
        self.init(delay=0)
        self.apply_ok([event('p1',0,'A',1,2),event('p2',0,'A',1,2)])  # p2 duplicate of A
        self.apply_ok([event('p2',1,'Z',100,1)])
        self.apply_ok([event('p1',1,'H',100,1)])  # watermark 100: window [0,10) closes
        r=self.apply_ok([event('p2',2,'LATE',5,4)])  # genuinely late, dropped from aggregation
        self.assertEqual([e['id'] for e in r['late_events']],['LATE'])
        self.assertEqual((r['accepted_count'],r['duplicate_events']),(3,1))
        r=self.erase_ok(['A','LATE'])
        self.assertEqual(r['late_events'],[])
        self.assertEqual(r['erased_event_ids'],['A','LATE'])
        self.assertEqual((r['accepted_count'],r['accepted_sum']),(2,2))  # Z and H remain
        conn=sqlite3.connect(self.db)
        try:
            # both partition records that carried A (p1 accepted, p2 duplicate) lost their payload
            for partition,offset in [('p1',0),('p2',0)]:
                digest,event_id,body=conn.execute('SELECT digest,event_id,body FROM records '
                                                  'WHERE partition=? AND offset=?',(partition,offset)).fetchone()
                self.assertEqual(len(digest),64);self.assertIsNone(event_id);self.assertIsNone(body)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM seen WHERE id IN ('A','LATE')").fetchone()[0],0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM late_events WHERE id='LATE'").fetchone()[0],0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM contributions WHERE id IN ('A','LATE')").fetchone()[0],0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM tombstones").fetchone()[0],2)
        finally:conn.close()
        # retransmission of every old partition-0 record stays a no-op even though bodies are gone
        for name in ('batch1.jsonl','batch2.jsonl','batch3.jsonl','batch4.jsonl'):
            self.assertEqual(self.apply(self.dir/name).returncode,0)
        r=self.report()
        self.assertEqual(r['accepted_count'],2)
        self.assertEqual((r['duplicate_events'],r['erased_event_ids']),(1,['A','LATE']))
        # a brand new offset that re-sends an erased id is refused, on either partition
        for partition,offset in [('p1',2),('p2',3)]:
            self.assertEqual(self.apply([event(partition,offset,'A',1,2)]).returncode,2)
        self.assertEqual(self.report()['accepted_count'],2)

    def test_no_plaintext_payload_in_sql(self):
        self.init(delay=0)
        self.apply_ok([event('p1',0,'SECRET',424242,987654321),event('p1',1,'KEEP',1,5)])
        out=self.dir/'e.json'
        r=self.erase(['SECRET'],out)
        self.assertEqual(r.returncode,0,r.stderr)
        payload=json.loads(out.read_text())
        self.assertEqual(payload['erased_event_ids'],['SECRET'])
        conn=sqlite3.connect(self.db)
        try:
            # payload tables carry neither the id nor the value of the erased event
            for table,column in [('seen','id'),('late_events','id'),('contributions','id'),
                                 ('records','event_id'),('records','body')]:
                rows_=conn.execute('SELECT %s FROM %s WHERE %s IS NOT NULL'%(column,table,column)).fetchall()
                text=json.dumps(rows_,default=str)
                self.assertNotIn('SECRET',text,table+'.'+column)
                self.assertNotIn('987654321',text,table+'.'+column)
            # event rows and record payloads for the erased id are gone
            for table in ('seen','late_events','contributions'):
                self.assertEqual(conn.execute('SELECT COUNT(*) FROM %s WHERE id=?'%table,('SECRET',)).fetchone()[0],0,table)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM records WHERE event_id='SECRET' OR body LIKE '%SECRET%'").fetchone()[0],0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM records WHERE body IS NULL").fetchone()[0],1)
            self.assertEqual(conn.execute("SELECT id FROM tombstones").fetchall(),[('SECRET',)])
            # the watermark ledger keeps the high-water mark by contract: it must never retreat
            self.assertEqual(conn.execute("SELECT value FROM meta WHERE key='watermark'").fetchone()[0],'null')
        finally:conn.close()
        self.assertNotIn('SECRET',json.dumps(payload['closed_windows']+payload['pending_windows']))
        self.assertNotIn('987654321',json.dumps(payload))

    def test_erase_requires_valid_ids_file(self):
        self.init()
        self.apply_ok([event('p1',0,'A',1,2)])
        out=self.dir/'e.json';out.write_text('prior')
        bad=self.dir/'bad.json';bad.write_text('{"ids": []}')  # wrong top-level field
        self.assertEqual(cli('stream','erase','--db',self.db,'--ids',bad,'--output',out).returncode,2)
        bad.write_text('{"event_ids": ["A"], "x": 1}')
        self.assertEqual(cli('stream','erase','--db',self.db,'--ids',bad,'--output',out).returncode,2)
        self.assertEqual(out.read_text(),'prior')
        bad.write_text('{"event_ids": ["A", "A"]}')  # duplicates are collapsed, not an error
        self.assertEqual(cli('stream','erase','--db',self.db,'--ids',bad,'--output',out).returncode,0)
        self.assertEqual(json.loads(out.read_text())['erased_event_ids'],['A'])
        for ids in ([1],[''],[3]):
            self.assertEqual(self.erase(ids).returncode,2)
        self.assertEqual(cli('stream','erase','--db',self.db,'--ids',self.ids_file(['KEEP']),
                             '--output',self.db).returncode,2)  # output would overwrite state
        link=self.dir/'link.json';link.symlink_to('nowhere')
        self.assertEqual(cli('stream','erase','--db',self.db,'--ids',self.ids_file(['KEEP']),
                             '--output',link).returncode,2)
        missing=self.dir/'missing.db'
        self.assertEqual(cli('stream','erase','--db',missing,'--ids',self.ids_file(['A']),
                             '--output',self.dir/'eo.json').returncode,2)
        self.assertEqual(cli('stream','erase','--db',self.db,'--ids',self.ids_file(['A']),
                             '--output',self.dir/'ok2.json').returncode,0)  # still idempotent afterwards

class Migration(EraseCase):
    SCHEMA1="""
    CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE partitions(name TEXT PRIMARY KEY, status TEXT NOT NULL, next_offset INTEGER NOT NULL, max_event_time INTEGER);
    CREATE TABLE records(partition TEXT NOT NULL, offset INTEGER NOT NULL, body TEXT NOT NULL, PRIMARY KEY(partition,offset));
    CREATE TABLE seen(id TEXT PRIMARY KEY, event_time INTEGER NOT NULL, value INTEGER NOT NULL);
    CREATE TABLE windows(start INTEGER PRIMARY KEY, count INTEGER NOT NULL, total INTEGER NOT NULL,
                         closed INTEGER NOT NULL DEFAULT 0, close_seq INTEGER);
    CREATE TABLE contributions(seq INTEGER PRIMARY KEY, start INTEGER NOT NULL, id TEXT NOT NULL);
    CREATE TABLE late_events(seq INTEGER PRIMARY KEY, id TEXT NOT NULL, event_time INTEGER NOT NULL,
                             value INTEGER NOT NULL, partition TEXT NOT NULL, watermark_at_arrival INTEGER NOT NULL);
    """
    def make_schema1(self,records,watermark=1,bodies=None):
        conn=sqlite3.connect(self.db);conn.executescript(self.SCHEMA1)
        for key,value in [('schema',1),('window_seconds',10),('out_of_order_seconds',0),
                          ('watermark',watermark),('duplicates',0)]:
            conn.execute('INSERT INTO meta VALUES (?,?)',(key,json.dumps(value)))
        conn.execute("INSERT INTO partitions VALUES ('p1','active',?,NULL)",(len(records),))
        for offset,record in enumerate(records):
            body=json.dumps(record,sort_keys=True,separators=(',',':')) if bodies is None else bodies[offset]
            conn.execute('INSERT INTO records VALUES (?,?,?)',('p1',offset,body))
            conn.execute('INSERT INTO seen VALUES (?,?,?)',(record['id'],record['event_time'],record['value']))
            start=record['event_time']//10*10
            conn.execute('INSERT INTO windows(start,count,total) VALUES (?,1,?) ON CONFLICT(start) DO UPDATE '
                         'SET count=count+1, total=total+?',(start,record['value'],record['value']))
            conn.execute('INSERT INTO contributions VALUES (?,?,?)',(offset,start,record['id']))
            conn.execute("UPDATE partitions SET max_event_time=MAX(COALESCE(max_event_time,?),?)",(record['event_time'],record['event_time']))
        conn.commit();conn.close()

    def test_schema1_migrates_in_place(self):
        records=[event('p1',0,'A',1,2),event('p1',1,'B',12,3)]
        self.make_schema1(records)
        r=self.report()
        self.assertEqual(r['erase_epoch'],0);self.assertEqual(r['erased_event_ids'],[])
        self.assertEqual(r['accepted_count'],2)
        conn=sqlite3.connect(self.db)
        try:
            self.assertEqual(json.loads(conn.execute("SELECT value FROM meta WHERE key='schema'").fetchone()[0]),2)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM tombstones").fetchone()[0],0)
            digests=[row[0] for row in conn.execute('SELECT digest FROM records ORDER BY offset')]
        finally:conn.close()
        bodies=[json.dumps(record,sort_keys=True,separators=(',',':')) for record in records]
        self.assertEqual(digests,[hashlib.sha256(body.encode()).hexdigest() for body in bodies])
        self.assertEqual(self.apply(records).returncode,0)  # retransmit: no-op
        conflict=self.write_batch([event('p1',0,'A',1,99)])
        self.assertEqual(self.apply(conflict).returncode,2)
        self.assertEqual(self.erase_ok(['A'])['erase_epoch'],1)  # and erasing works on the migrated state

    def test_migration_failure_rolls_back(self):
        records=[event('p1',0,'A',1,2)]
        self.make_schema1(records,bodies=['{not json'])
        out=self.dir/'r.json';out.write_text('prior')
        r=cli('stream','report','--db',self.db,'--output',out)
        self.assertEqual(r.returncode,2);self.assertEqual(out.read_text(),'prior')
        conn=sqlite3.connect(self.db)
        try:
            self.assertEqual(json.loads(conn.execute("SELECT value FROM meta WHERE key='schema'").fetchone()[0]),1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='tombstones'").fetchone()[0],0)
            self.assertEqual(conn.execute("SELECT body FROM records WHERE offset=0").fetchone()[0],'{not json')
        finally:conn.close()

class OwnPublication(EraseCase):
    def make_plan(self,protected,queries,name='plan.json'):
        p=self.dir/name;p.write_text(json.dumps({'queries':queries,'protected_ids':protected}));return p

    def test_reconcile_excludes_erased_and_reports_epoch(self):
        self.init(delay=0)
        self.apply_ok([event('p1',0,'A',1,2),event('p1',1,'B',12,2),event('p1',2,'C',20,2)])
        db_b=self.dir/'b.db';self.init(db=db_b)
        batch=self.write_batch([event('p1',0,'A',1,2),event('p1',1,'C',20,2)])
        self.assertEqual(cli('stream','apply','--db',db_b,'--batch',batch,'--output',self.dir/'rb.json').returncode,0)
        self.erase_ok(['B'])
        out=self.dir/'rec.json'
        r=cli('reconcile','--a',self.db,'--b',db_b,'--tolerance','0','--output',out)
        self.assertEqual(r.returncode,0,r.stderr)
        rep=json.loads(out.read_text())
        self.assertEqual(rep['sources']['a']['erase_epoch'],1)
        self.assertEqual(rep['sources']['b']['erase_epoch'],0)
        self.assertEqual(rep['sources']['a']['accepted_events'],2)
        self.assertEqual([e['id'] for e in rep['unmatched_a']],[])
        self.assertEqual([(p['a_id'],p['b_id']) for p in rep['pairs']],[('A','A'),('C','C')])
        self.assertNotIn('B',json.dumps(rep))

    def test_audit_excludes_erased_and_references_fail(self):
        self.init(delay=0)
        self.apply_ok([event('p1',0,'A',1,7),event('p1',1,'B',2,3),event('p1',2,'C',3,1)])
        self.erase_ok(['B'])
        plan=self.make_plan(['A'],[{'query_id':'q1','cost':5,'event_ids':['A','C']}])
        out=self.dir/'a.json'
        r=cli('audit','--db',self.db,'--plan',plan,'--output',out)
        self.assertEqual(r.returncode,0,r.stderr)
        rep=json.loads(out.read_text())
        self.assertEqual(rep['publication']['event_ids'],['A','C'])
        self.assertEqual(rep['erase_epoch'],1)
        for plan_obj,fragment,name in [({'queries':[{'query_id':'q1','cost':1,'event_ids':['B']}],'protected_ids':[]},
                                        'erased event','p1.json'),
                                       ({'queries':[],'protected_ids':['B']},'erased event','p2.json')]:
            bad=self.make_plan(plan_obj['protected_ids'],plan_obj['queries'],name)
            r=cli('audit','--db',self.db,'--plan',bad,'--output',self.dir/'ab.json')
            self.assertEqual(r.returncode,2);self.assertIn(fragment,r.stderr)

    def _forward(self,args):
        """Run a gated reader, let an erase commit, then release it: no stale result may publish."""
        args=list(args)
        out=Path(args[args.index('--output')+1])
        before=out.read_bytes()
        gate=self.gate_dir()
        proc=gated(*args,gate=gate,block='snapshot-taken')
        self.addCleanup(lambda:proc.poll() or proc.kill())
        wait_for(gate,'snapshot-taken')
        self.assertEqual(self.erase(['A']).returncode,0)  # commits while the reader is paused
        release(gate,'snapshot-taken')
        code,stdout,stderr=finish(proc)
        self.assertEqual(code,2,'reader published a stale snapshot: '+stderr)
        self.assertIn('erase epoch changed',stderr)
        self.assertEqual(out.read_bytes(),before)  # old report preserved byte for byte
        self.assertEqual(stdout,'')  # nothing went to stdout either
        return stderr

    def test_stream_report_does_not_publish_stale_snapshot(self):
        self.init(delay=0)
        self.apply_ok([event('p1',0,'A',1,2),event('p1',1,'B',12,3)])
        out=self.dir/'stream.json'
        self.assertEqual(cli('stream','report','--db',self.db,'--output',out).returncode,0)
        self._forward(('stream','report','--db',self.db,'--output',out))
        self.assertEqual(self.report()['erased_event_ids'],['A'])

    def test_reconcile_does_not_publish_stale_snapshot(self):
        self.init(delay=0)
        self.apply_ok([event('p1',0,'A',1,2),event('p1',1,'B',12,3)])
        db_b=self.dir/'b.db';self.init(db=db_b)
        out=self.dir/'rec.json'
        self.assertEqual(cli('reconcile','--a',self.db,'--b',db_b,'--tolerance','0','--output',out).returncode,0)
        self._forward(('reconcile','--a',self.db,'--b',db_b,'--tolerance','0','--output',out))

    def test_audit_does_not_publish_stale_snapshot(self):
        self.init(delay=0)
        self.apply_ok([event('p1',0,'A',1,2),event('p1',1,'B',12,3)])
        plan=self.make_plan([],[])
        out=self.dir/'audit.json'
        self.assertEqual(cli('audit','--db',self.db,'--plan',plan,'--output',out).returncode,0)
        self._forward(('audit','--db',self.db,'--plan',plan,'--output',out))

    def test_erase_waits_for_publication_validation_replace_and_stdout(self):
        self.init(delay=0)
        self.apply_ok([event('p1',0,'A',1,2),event('p1',1,'B',12,3)])
        out=self.dir/'stream.json'
        self.assertEqual(cli('stream','report','--db',self.db,'--output',out).returncode,0)
        prior=out.read_bytes()
        gate=self.gate_dir()
        proc=gated('stream','report','--db',self.db,'--output',out,gate=gate,block='validated')
        self.addCleanup(lambda:proc.poll() or proc.kill())
        wait_for(gate,'validated')  # reader holds the shared lock: epoch verified, not yet replaced
        eraser=subprocess.Popen([sys.executable,'-m','watermark_windows.cli','stream','erase',
                                 '--db',str(self.db),'--ids',str(self.ids_file(['A'])),
                                 '--output',str(self.dir/'e2.json')],
                                stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,
                                env={k:v for k,v in os.environ.items() if k!='WATERMARK_GATE_DIR'})
        try:
            time.sleep(0.6)
            self.assertIsNone(eraser.poll())  # erase cannot commit while publication is in flight
            self.assertIsNone(proc.poll())
            release(gate,'validated')  # reader saves, writes stdout, releases the lock
            code,stdout,stderr=finish(proc)
            self.assertEqual(code,0,stderr)
            self.assertEqual(out.read_bytes(),prior)  # replaced with the same pre-erase report
            self.assertEqual(json.loads(stdout)['erase_epoch'],0)  # published result is the pre-erase one
            ecode,eout,eerr=finish(eraser)
            self.assertEqual(ecode,0,eerr)
            self.assertEqual(json.loads(eout)['erase_epoch'],1)  # erase committed only after the reader finished
        finally:
            for p in (proc,eraser):
                if p.poll() is None:p.kill()

    def test_erase_waits_through_file_replace_and_stdout(self):
        # the erase is held until stdout is written: the whole guard spans replacement+stdout
        self.init(delay=0)
        self.apply_ok([event('p1',0,'A',1,2)])
        gate=self.gate_dir()
        proc=gated('stream','report','--db',self.db,'--output',self.dir/'s.json',gate=gate,block='saved')
        self.addCleanup(lambda:proc.poll() or proc.kill())
        wait_for(gate,'saved')  # report file already replaced; stdout not yet written
        eraser=subprocess.Popen([sys.executable,'-m','watermark_windows.cli','stream','erase',
                                 '--db',str(self.db),'--ids',str(self.ids_file(['A'])),
                                 '--output',str(self.dir/'e3.json')],
                                stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,
                                env={k:v for k,v in os.environ.items() if k!='WATERMARK_GATE_DIR'})
        try:
            time.sleep(0.6);self.assertIsNone(eraser.poll())
            release(gate,'saved')
            self.assertEqual(finish(proc)[0],0)
            self.assertEqual(finish(eraser)[0],0)
        finally:
            for p in (proc,eraser):
                if p.poll() is None:p.kill()

    def test_alias_paths_coordinate(self):
        self.init(delay=0)
        self.apply_ok([event('p1',0,'A',1,2)])
        alias=self.dir/'alias.db';alias.symlink_to(self.db.name)
        gate=self.gate_dir()
        proc=gated('audit','--db',alias,'--plan',self.make_plan([],[]),'--output',self.dir/'a.json',
                   gate=gate,block='validated')
        self.addCleanup(lambda:proc.poll() or proc.kill())
        wait_for(gate,'validated')  # reader locks the alias, so the real path must wait too
        eraser=subprocess.Popen([sys.executable,'-m','watermark_windows.cli','stream','erase',
                                 '--db',str(self.db),'--ids',str(self.ids_file(['A'])),
                                 '--output',str(self.dir/'ear.json')],
                                stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,
                                env={k:v for k,v in os.environ.items() if k!='WATERMARK_GATE_DIR'})
        try:
            time.sleep(0.6);self.assertIsNone(eraser.poll())
            release(gate,'validated')
            self.assertEqual(finish(proc)[0],0)
            self.assertEqual(finish(eraser)[0],0)
            self.assertEqual(json.loads((self.dir/'ear.json').read_text())['erase_epoch'],1)
        finally:
            for p in (proc,eraser):
                if p.poll() is None:p.kill()

    def test_alias_forward_detects_epoch(self):
        self.init(delay=0)
        self.apply_ok([event('p1',0,'A',1,2)])
        alias=self.dir/'alias.db';alias.symlink_to(self.db.name)
        out=self.dir/'alias.json'
        self.assertEqual(cli('stream','report','--db',alias,'--output',out).returncode,0)
        prior=out.read_bytes()
        gate=self.gate_dir()
        proc=gated('stream','report','--db',alias,'--output',out,gate=gate,block='snapshot-taken')
        self.addCleanup(lambda:proc.poll() or proc.kill())
        wait_for(gate,'snapshot-taken')
        self.assertEqual(self.erase(['A']).returncode,0)  # commits through the real path
        release(gate,'snapshot-taken')
        code,_,stderr=finish(proc)
        self.assertEqual(code,2,stderr);self.assertIn('erase epoch changed',stderr)
        self.assertEqual(out.read_bytes(),prior)


    def test_hardlink_alias_forward_detects_epoch(self):
        self.init(delay=0)
        self.apply_ok([event('p1',0,'A',1,2)])
        alias=self.dir/'hard.db';os.link(self.db,alias)
        out=self.dir/'hard.json'
        self.assertEqual(cli('stream','report','--db',alias,'--output',out).returncode,0)
        prior=out.read_bytes()
        gate=self.gate_dir()
        proc=gated('stream','report','--db',alias,'--output',out,gate=gate,block='snapshot-taken')
        self.addCleanup(lambda:proc.poll() or proc.kill())
        wait_for(gate,'snapshot-taken')
        self.assertEqual(self.erase(['A']).returncode,0)  # commits through the original path
        release(gate,'snapshot-taken')
        code,_,stderr=finish(proc)
        self.assertEqual(code,2,stderr);self.assertIn('erase epoch changed',stderr)
        self.assertEqual(out.read_bytes(),prior)

    def test_apply_does_not_trip_the_epoch_guard(self):
        # only erase advances the epoch; a late apply commit is not a reason to drop a report
        self.init(delay=0)
        self.apply_ok([event('p1',0,'B',12,3)])
        out=self.dir/'s.json'
        self.assertEqual(cli('stream','report','--db',self.db,'--output',out).returncode,0)
        gate=self.gate_dir()
        proc=gated('stream','report','--db',self.db,'--output',out,gate=gate,block='snapshot-taken')
        self.addCleanup(lambda:proc.poll() or proc.kill())
        wait_for(gate,'snapshot-taken')
        self.assertEqual(self.apply([event('p1',1,'C',1,1)]).returncode,0)
        release(gate,'snapshot-taken')
        code,stdout,stderr=finish(proc)
        self.assertEqual(code,0,stderr)
        self.assertEqual(json.loads(stdout)['erase_epoch'],0)


    def test_same_source_both_sides_detects_mid_run_erase(self):
        # reconcile A and B are the same file: snapshots are two separate reads, so an erase
        # landing between them must abort publication rather than publish a torn pairing
        self.init(delay=0)
        self.apply_ok([event('p1',0,'A',1,2),event('p1',1,'B',12,3)])
        out=self.dir/'self.json'
        self.assertEqual(cli('reconcile','--a',self.db,'--b',self.db,'--tolerance','0','--output',out).returncode,0)
        prior=out.read_bytes()
        gate=self.gate_dir()
        proc=gated('reconcile','--a',self.db,'--b',self.db,'--tolerance','0','--output',out,
                   gate=gate,block='between-snapshots')
        self.addCleanup(lambda:proc.poll() or proc.kill())
        wait_for(gate,'between-snapshots')  # A snapshot taken, B snapshot not yet taken
        self.assertEqual(self.erase(['A']).returncode,0)
        release(gate,'between-snapshots')
        code,stdout,stderr=finish(proc)
        self.assertEqual(code,2,stderr)
        self.assertIn('erase epoch changed',stderr)
        self.assertEqual(out.read_bytes(),prior)
        self.assertEqual(stdout,'')

    def test_same_source_both_sides_is_stable(self):
        self.init(delay=0)
        self.apply_ok([event('p1',0,'A',1,2),event('p1',1,'B',12,3)])
        out=self.dir/'self.json'
        self.assertEqual(cli('reconcile','--a',self.db,'--b',self.db,'--tolerance','0','--output',out).returncode,0)
        rep=json.loads(out.read_text())
        self.assertEqual((rep['sources']['a']['erase_epoch'],rep['sources']['b']['erase_epoch']),(0,0))
        self.assertEqual(rep['objective']['pair_count'],2)


class EraseAPI(unittest.TestCase):
    def test_direct_calls_expose_same_semantics(self):
        with tempfile.TemporaryDirectory() as td:
            db=Path(td)/'s.db';stream.init(db,['p1'],10,0)
            batch=Path(td)/'b.jsonl'
            batch.write_text(json.dumps(event('p1',0,'A',1,2))+'\n')
            first=stream.apply(db,batch)
            self.assertEqual(first['erase_epoch'],0)
            self.assertEqual(stream.erase(db,['A'])['erase_epoch'],1)
            self.assertEqual(stream.erase(db,['A'])['erase_epoch'],1)
            with self.assertRaises(ValueError):stream.erase(db,['A','nope'])
            report,epochs=reconcile.reconcile(db,db,0)
            self.assertEqual(epochs,(1,1))
            self.assertEqual(report['sources']['a']['erase_epoch'],1)

if __name__=='__main__':unittest.main()
