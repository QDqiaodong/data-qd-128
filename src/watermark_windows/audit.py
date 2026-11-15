"""Internal audit: minimum-cost query suppression for single-event value privacy.

One stream state database is read under a consistent read-only snapshot; only
accepted events (window contributions) take part.  A publication plan names
aggregate queries (0/1 incidence over accepted events, each with a positive
integer suppression cost) and the protected event ids.  The publication itself
discloses the full event id set plus every kept query (members and sums);
suppressed query sums stay sealed.  Every single event value is treated as an
unbounded rational unknown (no integer bounds are used), so a protected event
is leaked exactly when its unit vector lies in the row space of the published
query-event matrix A: the adversary then holds rational coefficients c with
cA = e_i and derives the value from the published sums.  Otherwise a rational
vector d with Ad = 0 and d_i = 1 certifies freedom: the value can move along d
without changing any published sum.

The audit first evaluates the original plan (all queries kept) event by event,
emitting either the identification certificate c with the derived value or the
freedom certificate d.  It then selects a minimum-total-cost suppression set so
that the kept matrix B admits a freedom certificate for every protected event;
ties are broken by the lexicographically smallest ascending query_id list.

The search is a depth-first branch and bound over at most 22 queries.  Rank
state is reused incrementally along the tree: each node carries the exact
echelon form of the kept rows plus one residual per still-identifiable
protected event, extended row by row.  All rank tests run modulo p = 2**61-1,
never in floating point: every minor of a 0/1 matrix with at most 23 rows is
below 23**(23/2) < p by Hadamard's bound, so such a minor vanishes over GF(p)
iff it vanishes over Q and the modular rank equals the rational rank exactly.
The winning kept matrix is certified over Q afterwards; emitted certificates
are exact rationals printed as reduced p/q, listing nonzero coefficients only.
"""
import argparse, hashlib, json, sqlite3, sys
from fractions import Fraction
from pathlib import Path
from urllib.parse import quote
from .common import ident, number, require, read, save
from .reconcile import canonical

MAX_EVENTS=80
MAX_QUERIES=22
MOD=(1<<61)-1  # prime; see the module docstring for the exactness argument

def _snapshot(path):
    """Accepted events and late ids of one stream state under a consistent read-only snapshot."""
    p=Path(path)
    if not p.exists():raise ValueError('state not initialized: '+str(p))
    conn=sqlite3.connect('file:%s?mode=ro'%quote(str(p.resolve()),safe='/'),uri=True,isolation_level=None)
    try:
        conn.execute('BEGIN')  # one read transaction: a consistent snapshot even if a writer commits later
        try:row=conn.execute("SELECT value FROM meta WHERE key='schema'").fetchone()
        except sqlite3.Error:
            raise ValueError('not a stream state database: '+str(p))
        if row is None or json.loads(row[0])!=1:raise ValueError('unsupported state schema: '+str(p))
        events=conn.execute('SELECT c.id, s.event_time, s.value FROM contributions c '
                            'JOIN seen s ON s.id=c.id ORDER BY c.seq').fetchall()
        total=conn.execute('SELECT COUNT(*) FROM contributions').fetchone()[0]
        if total!=len(events):raise ValueError('corrupt state: contribution without accepted event: '+str(p))
        late={r[0] for r in conn.execute('SELECT id FROM late_events')}
        conn.execute('COMMIT')
    finally:conn.close()
    return canonical([(eid,moment,value) for eid,moment,value in events]),late

def _bounded(value,limit,what):
    if not isinstance(value,list) or len(value)>limit:raise ValueError('expected array of at most %d %s'%(limit,what))
    return value

def _load_plan(path,known,late):
    """Validate the plan against the accepted event set; return queries sorted by query_id."""
    data,_=read(path)
    require(data,'queries protected_ids')
    queries=_bounded(data['queries'],MAX_QUERIES,'queries')
    protected=_bounded(data['protected_ids'],MAX_EVENTS,'protected event ids')
    def ref(eid):
        ident(eid)
        if eid not in known:
            if eid in late:raise ValueError('plan references late event: '+eid)
            raise ValueError('plan references unknown event: '+eid)
    seen=set();parsed=[]
    for q in queries:
        require(q,'query_id cost event_ids')
        qid=ident(q['query_id'])
        if qid in seen:raise ValueError('duplicate query_id: '+qid)
        seen.add(qid)
        cost=number(q['cost'],1,10**9)
        members=_bounded(q['event_ids'],MAX_EVENTS,'event ids per query')
        for eid in members:ident(eid)
        if len(set(members))!=len(members):raise ValueError('duplicate event id in query: '+qid)
        for eid in members:ref(eid)
        parsed.append((qid,cost,members))
    for eid in protected:ident(eid)
    if len(set(protected))!=len(protected):raise ValueError('duplicate protected event id')
    for eid in protected:ref(eid)
    parsed.sort(key=lambda q:q[0])  # canonical query order: ascending query_id
    return parsed,list(protected)

def _rref(matrix,ncols):
    """Reduced row echelon form over Q; returns (rows,pivots,transform) with rows = transform * matrix."""
    rows=[];pivots=[];trans=[]
    for r,source in enumerate(matrix):
        v=[Fraction(x) for x in source];t=[Fraction(1 if k==r else 0) for k in range(len(matrix))]
        for k,pc in enumerate(pivots):
            c=v[pc]
            if c:
                v=[x-c*y for x,y in zip(v,rows[k])];t=[x-c*y for x,y in zip(t,trans[k])]
        pc=next((k for k in range(ncols) if v[k]),None)
        if pc is None:continue
        inv=1/v[pc];v=[x*inv for x in v];t=[x*inv for x in t]
        for k in range(len(pivots)):
            c=rows[k][pc]
            if c:
                rows[k]=[x-c*y for x,y in zip(rows[k],v)];trans[k]=[x-c*y for x,y in zip(trans[k],t)]
        pos=0
        while pos<len(pivots) and pivots[pos]<pc:pos+=1
        pivots.insert(pos,pc);rows.insert(pos,v);trans.insert(pos,t)
    return rows,pivots,trans

def _reduce(rows,pivots,target,ncols):
    """Residual and echelon coefficients of target: target = residual + sum(coeffs[k]*rows[k])."""
    v=[Fraction(x) for x in target];coeffs=[Fraction(0)]*len(rows)
    for k,pc in enumerate(pivots):
        c=v[pc]
        if c:v=[x-c*y for x,y in zip(v,rows[k])];coeffs[k]=c
    return v,coeffs

def _kernel_certificate(rows,pivots,ncols,i):
    """Canonical d with rows*d = 0 and d[i] = 1, or None when e_i lies in the row space."""
    if i in pivots:
        k=pivots.index(i)
        if all(rows[k][j]==0 for j in range(ncols) if j!=i):return None  # the row is exactly e_i
        free=set(range(ncols))-set(pivots)
        f=min(j for j in free if rows[k][j]!=0)
        d=[Fraction(0)]*ncols;d[f]=-1/rows[k][f]
        for j,pc in enumerate(pivots):d[pc]=-rows[j][f]*d[f]
        assert d[i]==1
        return d
    d=[Fraction(0)]*ncols;d[i]=Fraction(1)
    for k,pc in enumerate(pivots):d[pc]=-rows[k][i]
    return d

def _insert(state,row):
    """Echelon of the kept rows plus one residual per at-risk protected event, all mod MOD.

    Returns the updated state after keeping row, or None when keeping row would
    identify a protected event (its residual collapses to zero)."""
    echelon,residuals=state
    v=list(row)
    for pc,prow in echelon:
        c=v[pc]
        if c:v=[(x-c*y)%MOD for x,y in zip(v,prow)]
    pc=next((k for k,x in enumerate(v) if x),None)
    if pc is None:return echelon,residuals  # dependent row: span unchanged
    inv=pow(v[pc],MOD-2,MOD)
    if inv!=1:v=[x*inv%MOD for x in v]
    pos=0
    while pos<len(echelon) and echelon[pos][0]<pc:pos+=1
    grown=echelon[:pos]+[(pc,v)]+echelon[pos:]
    updated=[]
    for r in residuals:
        c=r[pc]
        if c:r=[(x-c*y)%MOD for x,y in zip(r,v)]
        if not any(r):return None
        updated.append(r)
    return grown,updated

def _search(incidence,costs,qids,at_risk,ncols):
    """Minimum-cost suppression set; ties: lexicographically smallest ascending query_id list."""
    m=len(incidence)
    def blank():return ([],[[1 if k==i else 0 for k in range(ncols)] for i in at_risk])
    state=blank();suppressed=[]  # greedy incumbent: keep the expensive queries first
    for q in sorted(range(m),key=lambda q:(-costs[q],qids[q])):
        nxt=_insert(state,incidence[q])
        if nxt is None:suppressed.append(q)
        else:state=nxt
    best=[sum(costs[q] for q in suppressed),sorted(qids[q] for q in suppressed)]
    def dfs(pos,state,sup,cost):
        if cost>best[0] or (cost==best[0] and sup>=best[1]):return
        if pos==m:
            best[0]=cost;best[1]=sup;return
        nxt=_insert(state,incidence[pos])
        if nxt is not None:dfs(pos+1,nxt,sup,cost)
        if cost+costs[pos]<=best[0]:dfs(pos+1,state,sup+[qids[pos]],cost+costs[pos])
    dfs(0,blank(),[],0)
    return best

def _frac(v):return '%d/%d'%(v.numerator,v.denominator)

def _sparse(vec,ids):return {ids[j]:_frac(v) for j,v in enumerate(vec) if v}

def _canonical_sha256(events,queries,protected):
    """SHA256 over the canonical input; see docs/audit.md for the exact encoding."""
    digest=hashlib.sha256()
    def line(obj):
        digest.update(json.dumps(obj,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode('utf-8'))
        digest.update(b'\n')
    for eid,moment,value in events:line({'event_time':moment,'id':eid,'value':value})
    for qid,cost,members in queries:line({'cost':cost,'event_ids':sorted(members),'query_id':qid})
    line({'protected_ids':sorted(protected)})
    return digest.hexdigest()

def audit(db,plan_path):
    """Run the audit over one stream state database and one JSON plan; return the report."""
    events,late=_snapshot(db)
    if len(events)>MAX_EVENTS:raise ValueError('audit supports at most %d accepted events'%MAX_EVENTS)
    known={eid for eid,_,_ in events}
    queries,protected=_load_plan(plan_path,known,late)
    n=len(events);ids=[eid for eid,_,_ in events];index={eid:k for k,eid in enumerate(ids)}
    incidence=[];sums=[]
    for qid,cost,members in queries:
        row=[0]*n
        for eid in members:row[index[eid]]=1
        incidence.append(row)
        sums.append(sum(events[index[eid]][2] for eid in members))
    qids=[q[0] for q in queries]
    rows,pivots,trans=_rref(incidence,n)
    original=[];at_risk=[]
    for eid in protected:
        k=index[eid]
        residual,coeffs=_reduce(rows,pivots,[1 if j==k else 0 for j in range(n)],n)
        if any(residual):
            d=_kernel_certificate(rows,pivots,n,k)
            original.append({'event_id':eid,'identifiable':False,'kernel_certificate':_sparse(d,ids)})
        else:
            c=[Fraction(0)]*len(incidence)
            for j,cf in enumerate(coeffs):
                if cf:c=[x+cf*y for x,y in zip(c,trans[j])]
            derived=sum(cf*s for cf,s in zip(c,sums))
            if derived!=events[k][2]:raise ValueError('internal: derived value mismatch')
            original.append({'event_id':eid,'identifiable':True,
                             'coefficients':_sparse(c,qids),'derived_value':events[k][2]})
            at_risk.append(k)
    active=sorted({j for row in incidence for j,x in enumerate(row) if x}|set(at_risk))
    proj={c:k for k,c in enumerate(active)}
    projected=[[row[c] for c in active] for row in incidence]
    total,suppressed_ids=_search(projected,[q[1] for q in queries],qids,[proj[k] for k in at_risk],len(active))
    suppressed=set(suppressed_ids)
    kept_rows=[row for row,q in zip(incidence,qids) if q not in suppressed]
    krows,kpivots,_=_rref(kept_rows,n)
    kept_plan=[]
    for eid in protected:
        d=_kernel_certificate(krows,kpivots,n,index[eid])
        if d is None:raise ValueError('internal: kept plan identifies protected event')
        kept_plan.append({'event_id':eid,'kernel_certificate':_sparse(d,ids)})
    return {'schema_version':1,'input_sha256':_canonical_sha256(events,queries,protected),
            'publication':{'event_ids':ids,
                           'kept_queries':[{'query_id':qid,'cost':cost,'event_ids':members,'sum':sums[j]}
                                           for j,(qid,cost,members) in enumerate(queries) if qid not in suppressed]},
            'suppression':{'total_cost':total,
                           'suppressed_queries':[{'query_id':qid,'cost':cost,'event_ids':members}
                                                 for qid,cost,members in queries if qid in suppressed]},
            'original_plan':{'per_event':original},
            'kept_plan':{'per_event':kept_plan}}

def main(argv=None):
    parser=argparse.ArgumentParser(prog='watermark-windows audit',description=__doc__.splitlines()[0])
    parser.add_argument('--db',type=Path,required=True,help='stream state database (read-only)')
    parser.add_argument('--plan',type=Path,required=True,help='JSON publication plan')
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args(argv)
    try:
        out=args.output.resolve()
        if out==args.db.resolve() or out==args.plan.resolve():raise ValueError('output would overwrite input')
        report=audit(args.db,args.plan)
        save(args.output,report)
    except (ValueError,TypeError,KeyError,OSError,OverflowError,RecursionError,sqlite3.Error) as error:
        print(str(error),file=sys.stderr);return 2
    print(json.dumps(report,sort_keys=True,ensure_ascii=False,allow_nan=False));return 0

if __name__=='__main__':raise SystemExit(main())
