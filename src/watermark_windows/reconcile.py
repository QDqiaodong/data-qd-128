"""Cross-source reconciliation of accepted stream events.

Each source state database is read once under a consistent read-only
snapshot; only accepted events (window contributions) join the canonical
sequence sorted by (event_time, id).  Two events may pair when values are
equal and the absolute time difference is at most tolerance; pairing
indices strictly increase on both sides and each event pairs at most once.
The bi-objective (maximize pair count, then minimize total absolute time
difference) is solved with Hirschberg's linear-space divide and conquer:
no n-by-m table is ever stored, workspace besides input and output is
O(min(n,m) + log(max(n,m)+1)), and the pairing detail is reconstructed by
recursion.  Ties are broken deterministically so reruns are reproducible.
"""
import argparse, hashlib, json, sqlite3, sys
from bisect import bisect_left, bisect_right
from itertools import accumulate
from pathlib import Path
from urllib.parse import quote
from .common import number, save, gate, publication_guard

MAX_EVENT_TIME=10**12  # event_time bound from the stream contract; also the tolerance bound

def _snapshot(path):
    """Read accepted events and the erase epoch of one stream state by consistent snapshot.

    Erased event ids live in `tombstones` and have no `seen` row, so they never
    join a contribution; schema 1 states have neither table.
    """
    p=Path(path)
    if not p.exists():raise ValueError('state not initialized: '+str(p))
    conn=sqlite3.connect('file:%s?mode=ro'%quote(str(p.resolve()),safe='/'),uri=True,isolation_level=None)
    try:
        conn.execute('BEGIN')  # one read transaction: a consistent snapshot even if a writer commits later
        try:row=conn.execute("SELECT value FROM meta WHERE key='schema'").fetchone()
        except sqlite3.Error:
            raise ValueError('not a stream state database: '+str(p))
        if row is None or json.loads(row[0]) not in (1,2):
            raise ValueError('unsupported state schema: '+str(p))
        schema=json.loads(row[0])
        epoch_row=conn.execute("SELECT value FROM meta WHERE key='erase_epoch'").fetchone()
        epoch=0 if epoch_row is None else int(json.loads(epoch_row[0]))
        # schema 2 keeps erased ids as permanent tombstones: exclude them twice over, both
        # by the anti-join and because erase deletes the matching contribution atomically
        tombstone_guard=" AND NOT EXISTS (SELECT 1 FROM tombstones t WHERE t.id=c.id)" if schema>=2 else ""
        events=conn.execute('SELECT c.id, s.event_time, s.value FROM contributions c '
                            'JOIN seen s ON s.id=c.id WHERE 1=1%s ORDER BY c.seq'%tombstone_guard).fetchall()
        total=conn.execute('SELECT COUNT(*) FROM contributions c WHERE 1=1%s'%tombstone_guard).fetchone()[0]
        if total!=len(events):raise ValueError('corrupt state: contribution without accepted event: '+str(p))
        conn.execute('COMMIT')
    finally:conn.close()
    return [(eid,moment,value) for eid,moment,value in events],epoch

def canonical(events):
    """Sort accepted events by (event_time, id); ids are unique per source so the order is total."""
    return sorted(events,key=lambda e:(e[1],e[0]))

def canonical_sha256(events):
    """SHA256 over the canonical sequence; see docs/reconcile.md for the exact encoding."""
    digest=hashlib.sha256()
    for eid,moment,value in events:
        line=json.dumps({'id':eid,'event_time':moment,'value':value},
                        sort_keys=True,ensure_ascii=False,separators=(',',':'))
        digest.update(line.encode('utf-8'));digest.update(b'\n')
    return digest.hexdigest()

def _forward_row(a,alo,ahi,b,blo,bhi,b_times,tol,weight):
    """Last DP row for a[alo:ahi] against every prefix of b[blo:bhi]; row[j] scores a[alo:ahi] x b[blo:blo+j]."""
    prev=[0]*(bhi-blo+1)
    for i in range(alo,ahi):
        t,v=a[i];cand=prev[:]
        for j in range(bisect_left(b_times,t-tol,blo,bhi),bisect_right(b_times,t+tol,blo,bhi)):
            if b[j][1]==v:
                jj=j-blo;d=t-b[j][0];d=d if d>=0 else -d
                s=prev[jj]+weight-d
                if s>cand[jj+1]:cand[jj+1]=s
        prev=list(accumulate(cand,max))
    return prev

def _backward_row(a,alo,ahi,b,blo,bhi,b_times,tol,weight):
    """Last DP row for a[alo:ahi] against every suffix of b[blo:bhi]; row[j] scores a[alo:ahi] x b[blo+j:bhi]."""
    prev=[0]*(bhi-blo+1)
    for i in range(ahi-1,alo-1,-1):
        t,v=a[i];cand=prev[:]
        for j in range(bisect_left(b_times,t-tol,blo,bhi),bisect_right(b_times,t+tol,blo,bhi)):
            if b[j][1]==v:
                jj=j-blo;d=t-b[j][0];d=d if d>=0 else -d
                s=prev[jj+1]+weight-d
                if s>cand[jj]:cand[jj]=s
        cur=list(accumulate(cand[::-1],max));cur.reverse()
        prev=cur
    return prev

def _solve(a,alo,ahi,b,blo,bhi,b_times,tol,weight,out):
    """Append optimal pairs (global indices) for a[alo:ahi] x b[blo:bhi] to out; b is the shorter side."""
    n=ahi-alo;m=bhi-blo
    if n==0 or m==0:return
    if n==1:
        t,v=a[alo];best_j=-1;best_s=0
        for j in range(bisect_left(b_times,t-tol,blo,bhi),bisect_right(b_times,t+tol,blo,bhi)):
            if b[j][1]==v:
                d=t-b[j][0];d=d if d>=0 else -d
                s=weight-d
                if s>best_s:best_s=s;best_j=j  # first hit wins: smallest index on ties
        if best_j>=0:out.append((alo,best_j))
        return
    mid=(alo+ahi)//2
    fwd=_forward_row(a,alo,mid,b,blo,bhi,b_times,tol,weight)
    bwd=_backward_row(a,mid,ahi,b,blo,bhi,b_times,tol,weight)
    best_j=0;best_s=fwd[0]+bwd[0]
    for j in range(1,m+1):
        s=fwd[j]+bwd[j]
        if s>best_s:best_s=s;best_j=j  # strict: smallest split point on ties
    del fwd,bwd  # keep the recursion spine at O(log n): rows die before the child calls
    _solve(a,alo,mid,b,blo,blo+best_j,b_times,tol,weight,out)
    _solve(a,mid,ahi,b,blo+best_j,bhi,b_times,tol,weight,out)

def match(a,b,tol):
    """Optimal pairing between canonical sequences a and b of (event_time, value).

    Returns a list of index pairs (i, j), strictly increasing on both sides,
    maximizing pair count then minimizing total absolute time difference.
    Workspace besides the returned list is O(min(n,m) + log(max(n,m)+1)).
    """
    n,m=len(a),len(b)
    if n==0 or m==0:return []
    swapped=n<m
    if swapped:a,b,n,m=b,a,m,n  # keep DP rows short: b is always the smaller side
    weight=m*MAX_EVENT_TIME+1  # one pair outranks any achievable total time difference
    b_times=[x[0] for x in b]
    out=[]
    _solve(a,0,n,b,0,m,b_times,tol,weight,out)
    if swapped:out=[(j,i) for i,j in out]
    out.sort()
    return out

def build_report(events_a,events_b,tol,epochs=None):
    """Reconcile two canonical (sorted) event sequences into a report dict."""
    a=[(moment,value) for _,moment,value in events_a]
    b=[(moment,value) for _,moment,value in events_b]
    pairs=match(a,b,tol)
    used_a={i for i,_ in pairs};used_b={j for _,j in pairs}
    def event(seq,index):
        eid,moment,value=seq[index]
        return {'index':index,'id':eid,'event_time':moment,'value':value}
    detail=[]
    for i,j in pairs:
        ea,eb=events_a[i],events_b[j]
        detail.append({'a_index':i,'b_index':j,'a_id':ea[0],'b_id':eb[0],
                       'event_time_a':ea[1],'event_time_b':eb[1],'value':ea[2],
                       'time_delta':abs(ea[1]-eb[1])})
    report={'schema_version':1,'tolerance':tol,
            'objective':{'pair_count':len(pairs),
                         'total_abs_time_delta':sum(p['time_delta'] for p in detail)},
            'sources':{'a':{'accepted_events':len(events_a),'canonical_sha256':canonical_sha256(events_a)},
                       'b':{'accepted_events':len(events_b),'canonical_sha256':canonical_sha256(events_b)}},
            'pairs':detail,
            'unmatched_a':[event(events_a,i) for i in range(len(events_a)) if i not in used_a],
            'unmatched_b':[event(events_b,j) for j in range(len(events_b)) if j not in used_b]}
    if epochs is not None:
        report['sources']['a']['erase_epoch']=epochs[0]
        report['sources']['b']['erase_epoch']=epochs[1]
    return report

def reconcile(db_a,db_b,tol):
    """Compute the reconciliation report and the snapshot epochs it depends on."""
    number(tol,0,MAX_EVENT_TIME)
    events_a,epoch_a=_snapshot(db_a)
    gate('between-snapshots')
    events_b,epoch_b=_snapshot(db_b)
    return build_report(canonical(events_a),canonical(events_b),tol,(epoch_a,epoch_b)),(epoch_a,epoch_b)

def main(argv=None):
    parser=argparse.ArgumentParser(prog='watermark-windows reconcile',description=__doc__.splitlines()[0])
    parser.add_argument('--a',type=Path,required=True,help='first stream state database (read-only)')
    parser.add_argument('--b',type=Path,required=True,help='second stream state database (read-only)')
    parser.add_argument('--tolerance',type=int,required=True,help='non-negative integer, max abs event_time difference')
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args(argv)
    try:
        number(args.tolerance,0,MAX_EVENT_TIME)
        out=args.output.resolve()
        if out==args.a.resolve() or out==args.b.resolve():
            raise ValueError('output would overwrite state database')
        report,epochs=reconcile(args.a,args.b,args.tolerance)
        gate('snapshot-taken')
        with publication_guard([(args.a,epochs[0]),(args.b,epochs[1])]):
            gate('validated')
            save(args.output,report)
            gate('saved')
            print(json.dumps(report,sort_keys=True,ensure_ascii=False,allow_nan=False),flush=True)
    except (ValueError,TypeError,KeyError,OSError,OverflowError,RecursionError,sqlite3.Error) as error:
        print(str(error),file=sys.stderr);return 2
    return 0

if __name__=='__main__':raise SystemExit(main())
