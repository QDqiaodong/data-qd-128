
import json,subprocess,sys,tempfile,unittest
from pathlib import Path

def call(*args):
    return subprocess.run([sys.executable,'-m','watermark_windows.cli',*(str(a) for a in args)],
                          capture_output=True,text=True)

def event(partition,offset,eid,moment,value):
    return {'partition':partition,'offset':offset,'kind':'event','id':eid,'event_time':moment,'value':value}

def marker(partition,offset,kind):
    return {'partition':partition,'offset':offset,'kind':kind}

class Stream(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.dir=Path(self.tmp.name);self.db=self.dir/'state.db';self.n=0
    def init(self,partitions='p1,p2',width=10,delay=5,db=None):
        return call('stream','init','--db',db or self.db,'--partitions',partitions,
                    '--window-seconds',width,'--out-of-order-seconds',delay)
    def batch(self,records,raw_tail=None):
        self.n+=1;path=self.dir/('batch%d.jsonl'%self.n)
        text=''.join(json.dumps(r)+'\n' for r in records)
        path.write_text(text if raw_tail is None else text+raw_tail);return path
    def apply(self,batch,out):
        return call('stream','apply','--db',self.db,'--batch',batch,'--output',out)
    def report(self,out,db=None):
        return call('stream','report','--db',db or self.db,'--output',out)
    def state(self,out):
        r=self.report(out);self.assertEqual(r.returncode,0,r.stderr);return json.loads(Path(out).read_text())

    def test_partition_blocking(self):
        self.assertEqual(self.init().returncode,0)
        b1=self.batch([event('p1',0,'A',1,2),event('p1',1,'B',12,3)])
        b2=self.batch([event('p2',0,'X',2,1),event('p2',1,'Y',22,1)])
        b3=self.batch([event('p1',2,'C',25,4)])
        out1,out2,out3=(self.dir/('r%d.json'%i) for i in (1,2,3))
        self.assertEqual(self.apply(b1,out1).returncode,0)
        r=json.loads(out1.read_text())
        self.assertIsNone(r['watermark']);self.assertEqual(r['closed_windows'],[])  # p2未见事件，阻塞全局水位
        self.assertEqual(self.apply(b2,out2).returncode,0)
        r=json.loads(out2.read_text())
        self.assertEqual(r['watermark'],7);self.assertEqual(r['closed_windows'],[])
        self.assertEqual(self.apply(b3,out3).returncode,0)
        r=json.loads(out3.read_text())
        self.assertEqual(r['watermark'],17)
        self.assertEqual(r['closed_windows'],[{'start':0,'end':10,'count':2,'sum':3,'event_ids':['A','X'],'closed_by':'watermark'}])
        self.assertEqual([(w['start'],w['event_ids']) for w in r['pending_windows']],[(10,['B']),(20,['Y','C'])])
        self.assertEqual((r['accepted_count'],r['accepted_sum'],r['max_event_time']),(5,11,25))
        self.assertEqual(r['partitions']['p1'],{'status':'active','next_offset':3,'max_event_time':25,'watermark':20})
        self.assertEqual(r['partitions']['p2'],{'status':'active','next_offset':2,'max_event_time':22,'watermark':17})
        rep=self.dir/'again.json';self.assertEqual(self.report(rep).returncode,0)
        self.assertEqual(rep.read_bytes(),out3.read_bytes())  # report与apply发布逐字节一致

    def test_idle_resume(self):
        self.assertEqual(self.init(delay=2).returncode,0)
        steps=[self.batch([event('p1',0,'A',1,2),event('p1',1,'B',12,3)]),
               self.batch([marker('p2',0,'idle')]),
               self.batch([event('p1',2,'C',13,1)]),
               self.batch([marker('p2',1,'resume'),event('p1',3,'D',30,5)]),
               self.batch([event('p2',2,'E',5,9),event('p2',3,'F',15,1),event('p2',4,'G',40,7)]),
               self.batch([marker('p1',4,'idle'),marker('p2',5,'idle')]),
               self.batch([marker('p1',5,'resume')])]
        outs=[self.dir/('s%d.json'%i) for i in range(len(steps))]
        for batch,out in zip(steps,outs):self.assertEqual(self.apply(batch,out).returncode,0)
        r=json.loads(outs[1].read_text());self.assertEqual(r['watermark'],10)  # p2 idle解除阻塞
        self.assertEqual([w['start'] for w in r['closed_windows']],[0])
        r=json.loads(outs[3].read_text());self.assertEqual(r['watermark'],11)  # resume后p2无事件，重新阻塞
        r=json.loads(outs[4].read_text())
        self.assertEqual(r['watermark'],28)  # p2局部水位3不拉低全局11
        self.assertEqual(r['late_events'],[{'id':'E','event_time':5,'value':9,'partition':'p2',
                                            'reason':'window_already_closed','watermark_at_arrival':11}])
        r=json.loads(outs[5].read_text());self.assertEqual(r['watermark'],38)  # 全idle不推进
        r=json.loads(outs[6].read_text())  # resume不回退全局水位
        self.assertEqual(r['watermark'],38)
        self.assertEqual([(w['start'],w['event_ids']) for w in r['closed_windows']],[(0,['A']),(10,['B','C','F'])])
        self.assertEqual([(w['start'],w['event_ids']) for w in r['pending_windows']],[(30,['D']),(40,['G'])])
        self.assertEqual((r['accepted_count'],r['accepted_sum'],r['duplicate_events']),(6,19,0))
        self.assertEqual(r['partitions']['p1']['status'],'active')
        self.assertEqual(r['partitions']['p2']['status'],'idle')
        self.assertTrue(all(w['closed_by']=='watermark' for w in r['closed_windows']))  # 结束不flush

    def test_retransmission(self):
        self.assertEqual(self.init().returncode,0)
        b1=self.batch([event('p1',0,'A',1,2),event('p1',1,'B',12,3)])
        out1=self.dir/'r1.json';self.assertEqual(self.apply(b1,out1).returncode,0)
        out1b=self.dir/'r1b.json';self.assertEqual(self.apply(b1,out1b).returncode,0)
        self.assertEqual(out1b.read_bytes(),out1.read_bytes())  # 一致重传不改变报告
        bad=self.batch([event('p1',0,'A',1,99)])  # 同offset不同内容：冲突回滚
        self.assertEqual(self.apply(bad,self.dir/'rx.json').returncode,2)
        out2=self.dir/'r2.json';self.assertEqual(self.report(out2).returncode,0)
        self.assertEqual(out2.read_bytes(),out1.read_bytes())
        mixed=self.batch([event('p1',0,'A',1,2),event('p1',1,'B',12,3),event('p1',2,'C',25,4)])
        out3=self.dir/'r3.json';self.assertEqual(self.apply(mixed,out3).returncode,0)  # 重传与新记录混批
        r=json.loads(out3.read_text())
        self.assertEqual(r['partitions']['p1']['next_offset'],3)
        self.assertEqual([(w['start'],w['event_ids']) for w in r['pending_windows']],[(0,['A']),(10,['B']),(20,['C'])])

    def test_global_id_dedup_and_conflict(self):
        self.assertEqual(self.init().returncode,0)
        self.assertEqual(self.apply(self.batch([event('p1',0,'A',1,2)]),self.dir/'r1.json').returncode,0)
        out=self.dir/'r2.json'  # 跨分区同ID同内容：去重
        self.assertEqual(self.apply(self.batch([event('p2',0,'A',1,2)]),out).returncode,0)
        r=json.loads(out.read_text())
        self.assertEqual((r['duplicate_events'],r['accepted_count']),(1,1))
        self.assertEqual(self.apply(self.batch([event('p2',1,'A',1,3)]),self.dir/'rx.json').returncode,2)  # 同ID冲突
        r=self.state(self.dir/'r3.json')
        self.assertEqual((r['duplicate_events'],r['partitions']['p2']['next_offset']),(1,1))

    def test_offset_gap_rolls_back(self):
        self.assertEqual(self.init().returncode,0)
        self.assertEqual(self.apply(self.batch([event('p1',1,'A',1,2)]),self.dir/'r1.json').returncode,2)
        gap=self.batch([event('p1',0,'A',1,2),event('p1',2,'B',12,3)])
        self.assertEqual(self.apply(gap,self.dir/'r2.json').returncode,2)
        r=self.state(self.dir/'r3.json')
        self.assertEqual((r['partitions']['p1']['next_offset'],r['accepted_count']),(0,0))

    def test_bad_tail_rolls_back_whole_batch(self):
        self.assertEqual(self.init().returncode,0)
        bad=self.batch([event('p1',0,'A',1,2),event('p1',1,'B',12,3)],raw_tail='{oops\n')
        self.assertEqual(self.apply(bad,self.dir/'r1.json').returncode,2)
        extra=self.batch([event('p1',0,'A',1,2)],raw_tail=json.dumps({'partition':'p1','offset':1,'kind':'idle','x':1})+'\n')
        self.assertEqual(self.apply(extra,self.dir/'r2.json').returncode,2)
        r=self.state(self.dir/'r3.json')
        self.assertEqual((r['partitions']['p1']['next_offset'],r['accepted_count']),(0,0))
        ok=self.batch([event('p1',0,'A',1,2)])  # 回滚干净后可重放
        out=self.dir/'r4.json';self.assertEqual(self.apply(ok,out).returncode,0)
        self.assertEqual(json.loads(out.read_text())['partitions']['p1']['next_offset'],1)

    def test_event_on_idle_partition_rolls_back(self):
        self.assertEqual(self.init().returncode,0)
        self.assertEqual(self.apply(self.batch([marker('p2',0,'idle')]),self.dir/'r1.json').returncode,0)
        self.assertEqual(self.apply(self.batch([event('p2',1,'X',1,1)]),self.dir/'r2.json').returncode,2)
        r=self.state(self.dir/'r3.json')
        self.assertEqual((r['partitions']['p2']['status'],r['partitions']['p2']['next_offset']),('idle',1))
        out=self.dir/'r4.json'  # 已idle再idle合法
        self.assertEqual(self.apply(self.batch([marker('p2',1,'idle')]),out).returncode,0)
        self.assertEqual(json.loads(out.read_text())['partitions']['p2']['next_offset'],2)

    def test_late_id_still_deduplicated(self):
        self.assertEqual(self.init(delay=2).returncode,0)
        b1=self.batch([event('p1',0,'A',1,2),event('p1',1,'B',30,5),marker('p2',0,'idle')])
        self.assertEqual(self.apply(b1,self.dir/'r1.json').returncode,0)  # 水位28，[0,10)关闭
        b2=self.batch([event('p1',2,'L',5,9)])
        out=self.dir/'r2.json';self.assertEqual(self.apply(b2,out).returncode,0)  # L迟到丢弃
        r=json.loads(out.read_text())
        self.assertEqual([e['id'] for e in r['late_events']],['L'])
        b3=self.batch([event('p1',3,'L',5,9)])  # 迟到ID一致重到：计重复，不再记迟到
        out=self.dir/'r3.json';self.assertEqual(self.apply(b3,out).returncode,0)
        r=json.loads(out.read_text())
        self.assertEqual((len(r['late_events']),r['duplicate_events'],r['accepted_count']),(1,1,2))
        self.assertEqual(self.apply(self.batch([event('p1',4,'L',5,8)]),self.dir/'rx.json').returncode,2)  # 迟到ID冲突

    def test_recovery_after_commit_before_publish(self):
        self.assertEqual(self.init(delay=2).returncode,0)
        b1=self.batch([event('p1',0,'A',1,2),event('p2',0,'X',3,1),event('p1',1,'B',12,3)])
        b2=self.batch([event('p2',1,'Y',20,5)])
        out1=self.dir/'r1.json';self.assertEqual(self.apply(b1,out1).returncode,0)
        link=self.dir/'link.json';link.symlink_to('nowhere')  # 发布失败，但批次已提交
        self.assertEqual(self.apply(b2,link).returncode,2)
        b1.unlink();b2.unlink()  # 恢复不读旧日志
        out2=self.dir/'r2.json';self.assertEqual(self.report(out2).returncode,0)
        other=self.dir/'other';self.assertEqual(self.init(delay=2,db=other).returncode,0)  # 不中断参照
        ref1=self.dir/'ref1.json';ref2=self.dir/'ref2.json'
        b1ref=self.batch([event('p1',0,'A',1,2),event('p2',0,'X',3,1),event('p1',1,'B',12,3)])
        b2ref=self.batch([event('p2',1,'Y',20,5)])
        run=lambda batch,out:subprocess.run([sys.executable,'-m','watermark_windows.cli','stream','apply',
            '--db',str(other),'--batch',str(batch),'--output',str(out)],capture_output=True,text=True)
        self.assertEqual(run(b1ref,ref1).returncode,0);self.assertEqual(run(b2ref,ref2).returncode,0)
        self.assertEqual(out1.read_bytes(),ref1.read_bytes())
        self.assertEqual(out2.read_bytes(),ref2.read_bytes())  # 恢复报告与不中断逐字节一致
        r=json.loads(out2.read_text())
        self.assertEqual(r['closed_windows'],[{'start':0,'end':10,'count':2,'sum':3,'event_ids':['A','X'],'closed_by':'watermark'}])
        again=self.dir/'r3.json';self.assertEqual(self.apply(b2ref,again).returncode,0)  # 恢复后重传
        self.assertEqual(again.read_bytes(),out2.read_bytes())  # 重传不改变报告

    def test_init_and_output_safety(self):
        self.assertEqual(self.init().returncode,0)
        self.assertEqual(self.init().returncode,2)  # 重复init拒绝
        for partitions,width,delay in [('p1,p1',10,5),('',10,5),('p1',0,5),('p1',10,-1)]:
            self.assertEqual(self.init(partitions=partitions,width=width,delay=delay,db=self.dir/'x.db').returncode,2)
        missing=self.dir/'missing.db'
        self.assertEqual(self.apply(self.batch([]),self.dir/'ro.json').returncode,0)  # 空批合法
        self.assertEqual(self.report(self.dir/'rr.json',db=missing).returncode,2)
        self.assertEqual(call('stream','apply','--db',missing,'--batch',self.batch([]),'--output',self.dir/'rn.json').returncode,2)
        corrupt=self.dir/'corrupt.db';corrupt.write_text('not sqlite')
        self.assertEqual(self.report(self.dir/'rc.json',db=corrupt).returncode,2)
        self.assertEqual(self.report(self.db).returncode,2)  # 输出覆盖状态库拒绝
        link=self.dir/'link.json';link.symlink_to('nowhere')
        self.assertEqual(self.report(link).returncode,2)
        unknown=self.batch([event('p9',0,'A',1,2)])
        self.assertEqual(self.apply(unknown,self.dir/'ru.json').returncode,2)
        for record in [{'partition':'p1','offset':0,'kind':'event','id':'A','event_time':1},
                       {'partition':'p1','offset':-1,'kind':'idle'},
                       {'partition':'p1','offset':0,'kind':'noop'}]:
            self.assertEqual(self.apply(self.batch([record]),self.dir/'rv.json').returncode,2)

if __name__=='__main__':unittest.main()
