
import copy,json,unittest,tempfile,subprocess,sys,hashlib
from pathlib import Path
from watermark_windows.model import analyze
ROOT=Path(__file__).resolve().parents[1]
def sample():return json.loads((ROOT/'fixtures/input.json').read_text())
class Business(unittest.TestCase):
    def bad(self,d):
        with self.assertRaises((ValueError,TypeError)):analyze(d)
    def test_unknown_top_field(self):
        d=sample();d['unexpected']=1;self.bad(d)

    def test_sample(self):
        r=analyze(sample());self.assertEqual((r['accepted_count'],r['accepted_sum'],r['duplicate_events']),(4,14,1));self.assertEqual([w['sum'] for w in r['closed_windows']],[6,3,5]);self.assertEqual(r['late_events'][0]['id'],'E')
    def test_pending(self):
        d=sample();d['flush']=False;r=analyze(d);self.assertEqual([w['sum'] for w in r['pending_windows']],[3,5]);self.assertEqual(r['watermark'],15)
    def test_empty(self):
        d=sample();d['events']=[];r=analyze(d);self.assertIsNone(r['watermark']);self.assertEqual(r['accepted_count'],0)
    def test_conflict(self):
        d=sample();d['events'][-1]['value']=99;self.bad(d)
    def test_exact_late_boundary(self):
        d=sample();d['events']=[{'id':'a','event_time':15,'value':1},{'id':'b','event_time':9,'value':2}];r=analyze(d);self.assertEqual(r['accepted_sum'],1);self.assertEqual(len(r['late_events']),1)
    def test_half_open(self):
        d=sample();d['events']=[{'id':'a','event_time':10,'value':1}];r=analyze(d);self.assertEqual(r['closed_windows'][0]['start'],10)
    def test_negative_watermark(self):
        d=sample();d['events']=d['events'][:1];r=analyze(d);self.assertEqual(r['watermark'],-4)
    def test_bad_width(self):
        d=sample();d['window_seconds']=0;self.bad(d)

class Command(unittest.TestCase):
    def call(self,source,output):
        return subprocess.run([sys.executable,'-m','watermark_windows.cli','--source',str(source),'--output',str(output)],capture_output=True,text=True)
    def test_installed_cli_replay(self):
        with tempfile.TemporaryDirectory() as td:
            out=Path(td)/'out.json';src=ROOT/'fixtures/input.json';r=self.call(src,out);self.assertEqual(r.returncode,0,r.stderr);first=out.read_bytes();report=json.loads(first);self.assertEqual(report['source_sha256'],hashlib.sha256(src.read_bytes()).hexdigest());self.assertEqual(self.call(src,out).returncode,0);self.assertEqual(first,out.read_bytes())
    def test_failure_preserves_output(self):
        with tempfile.TemporaryDirectory() as td:
            src=Path(td)/'bad.json';out=Path(td)/'out.json';out.write_text('prior')
            for body in ['{}','{"x":1,"x":2}','NaN']:
                src.write_text(body);self.assertEqual(self.call(src,out).returncode,2);self.assertEqual(out.read_text(),'prior')
    def test_output_safety(self):
        with tempfile.TemporaryDirectory() as td:
            src=Path(td)/'in.json';src.write_bytes((ROOT/'fixtures/input.json').read_bytes());before=src.read_bytes();self.assertEqual(self.call(src,src).returncode,2);self.assertEqual(src.read_bytes(),before);out=Path(td)/'link.json';out.symlink_to(src);self.assertEqual(self.call(src,out).returncode,2);self.assertEqual(src.read_bytes(),before)
