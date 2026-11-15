"""Partitioned incremental windows with SQLite-backed resumable state.

Each partition carries its own offset sequence from 0; batches apply atomically.
Local watermark = partition max event time - delay; the global watermark is the
minimum over active partitions, blocks on unseen partitions, never retreats.

Event ids can be erased logically: one transaction removes the plaintext payload
from `seen`, `late_events` and the stored record bodies, deducts the window
contributions, inserts a permanent tombstone per id and advances the erase epoch.
Erased ids can never be accepted again on any partition, while old offsets keep
the canonical record SHA256 so identical retransmissions stay no-ops.
"""
import argparse, hashlib, json, sqlite3, sys
from pathlib import Path
from .common import (ident, number, require, rows, loads, save, read,
                     gate, publication_guard, state_lock)

KINDS=('event','idle','resume')
SCHEMA_VERSION=2
SCHEMA="""
CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE partitions(
  name TEXT PRIMARY KEY,
  status TEXT NOT NULL CHECK(status IN ('active','idle')),
  next_offset INTEGER NOT NULL,
  max_event_time INTEGER);
CREATE TABLE records(
  partition TEXT NOT NULL,
  offset INTEGER NOT NULL,
  digest TEXT NOT NULL,
  event_id TEXT,
  body TEXT,
  PRIMARY KEY(partition,offset));
CREATE TABLE seen(
  id TEXT PRIMARY KEY,
  event_time INTEGER NOT NULL,
  value INTEGER NOT NULL);
CREATE TABLE windows(
  start INTEGER PRIMARY KEY,
  count INTEGER NOT NULL,
  total INTEGER NOT NULL,
  closed INTEGER NOT NULL DEFAULT 0,
  close_seq INTEGER);
CREATE TABLE contributions(
  seq INTEGER PRIMARY KEY,
  start INTEGER NOT NULL,
  id TEXT NOT NULL);
CREATE TABLE late_events(
  seq INTEGER PRIMARY KEY,
  id TEXT NOT NULL,
  event_time INTEGER NOT NULL,
  value INTEGER NOT NULL,
  partition TEXT NOT NULL,
  watermark_at_arrival INTEGER NOT NULL);
CREATE TABLE tombstones(
  id TEXT PRIMARY KEY,
  erased_at INTEGER NOT NULL);
"""

def _meta(conn,key):
    row=conn.execute('SELECT value FROM meta WHERE key=?',(key,)).fetchone()
    return json.loads(row[0])

def _set_meta(conn,key,value):
    conn.execute('UPDATE meta SET value=? WHERE key=?',(json.dumps(value),key))

def _params(conn):
    return _meta(conn,'window_seconds'),_meta(conn,'out_of_order_seconds')

def _canonical(record):
    return json.dumps(record,sort_keys=True,ensure_ascii=False,allow_nan=False,separators=(',',':'))

def _digest(body):
    return hashlib.sha256(body.encode('utf-8')).hexdigest()

def _next_seq(conn,table,column='seq'):
    return conn.execute('SELECT COALESCE(MAX(%s),-1)+1 FROM %s'%(column,table)).fetchone()[0]

def _validate_record(obj):
    if not isinstance(obj,dict):raise ValueError('expected record object')
    kind=obj.get('kind')
    if kind not in KINDS:raise ValueError('kind must be event, idle or resume')
    if kind=='event':
        require(obj,'partition offset kind id event_time value')
        ident(obj['id']);number(obj['event_time'],0,10**12);number(obj['value'],-10**9,10**9)
    else:require(obj,'partition offset kind')
    ident(obj['partition'])
    if type(obj['offset']) is not int or obj['offset']<0:raise ValueError('offset must be a non-negative integer')

def _read_batch(path):
    with Path(path).open('rb') as stream:raw=stream.read(8_000_001)
    if len(raw)>8_000_000:raise ValueError('JSONL exceeds 8 MB')
    lines=[line for line in raw.decode('utf-8').split('\n') if line.strip()]
    if len(lines)>20000:raise ValueError('batch exceeds 20000 records')
    return lines

def _advance(conn):
    width,delay=_params(conn)
    rows_=conn.execute("SELECT max_event_time FROM partitions WHERE status='active'").fetchall()
    if not rows_ or any(r[0] is None for r in rows_):return
    candidate=min(r[0] for r in rows_)-delay
    watermark=_meta(conn,'watermark')
    if watermark is not None and candidate<=watermark:return
    _set_meta(conn,'watermark',candidate)
    seq=_next_seq(conn,'windows','close_seq')
    for (start,) in conn.execute('SELECT start FROM windows WHERE closed=0 AND start+?<=? ORDER BY start',(width,candidate)).fetchall():
        conn.execute('UPDATE windows SET closed=1, close_seq=? WHERE start=?',(seq,start));seq+=1

def _apply_event(conn,record):
    eid,moment,value=record['id'],record['event_time'],record['value']
    row=conn.execute('SELECT event_time, value FROM seen WHERE id=?',(eid,)).fetchone()
    if row:
        if row!=(moment,value):raise ValueError('conflicting event ID: '+eid)
        _set_meta(conn,'duplicates',_meta(conn,'duplicates')+1);return
    conn.execute('INSERT INTO seen VALUES (?,?,?)',(eid,moment,value))
    width,_=_params(conn);start=moment//width*width;watermark=_meta(conn,'watermark')
    if watermark is not None and start+width<=watermark:
        conn.execute('INSERT INTO late_events VALUES (?,?,?,?,?,?)',
            (_next_seq(conn,'late_events'),eid,moment,value,record['partition'],watermark));return
    conn.execute('INSERT INTO windows(start,count,total) VALUES (?,1,?) '
                 'ON CONFLICT(start) DO UPDATE SET count=count+1, total=total+?',(start,value,value))
    conn.execute('INSERT INTO contributions VALUES (?,?,?)',(_next_seq(conn,'contributions'),start,eid))
    conn.execute('UPDATE partitions SET max_event_time=MAX(COALESCE(max_event_time,?), ?) WHERE name=?',
                 (moment,moment,record['partition']))
    _advance(conn)

def _apply_record(conn,record,known):
    partition,offset,kind=record['partition'],record['offset'],record['kind']
    if partition not in known:raise ValueError('unknown partition: '+partition)
    status,next_offset=conn.execute('SELECT status, next_offset FROM partitions WHERE name=?',(partition,)).fetchone()
    if offset>next_offset:raise ValueError('offset gap on partition %s: expected %d, got %d'%(partition,next_offset,offset))
    body=_canonical(record);digest=_digest(body)
    if offset<next_offset:
        stored=conn.execute('SELECT digest FROM records WHERE partition=? AND offset=?',(partition,offset)).fetchone()
        if stored is None or stored[0]!=digest:
            raise ValueError('conflicting retransmission on partition %s offset %d'%(partition,offset))
        return  # identical retransmission, even when its payload has been erased: no-op
    if kind=='event':
        if status=='idle':raise ValueError('event on idle partition: '+partition)
        if conn.execute('SELECT 1 FROM tombstones WHERE id=?',(record['id'],)).fetchone():
            raise ValueError('event ID was erased: '+record['id'])
        _apply_event(conn,record)
    else:
        conn.execute('UPDATE partitions SET status=? WHERE name=?',('idle' if kind=='idle' else 'active',partition))
        _advance(conn)
    conn.execute('INSERT INTO records VALUES (?,?,?,?,?)',
                 (partition,offset,digest,record.get('id') if kind=='event' else None,body))
    conn.execute('UPDATE partitions SET next_offset=? WHERE name=?',(offset+1,partition))

def _migrate(conn):
    """Upgrade a schema 1 database in one transaction: record digests, tombstones, epoch."""
    conn.execute('BEGIN IMMEDIATE')
    try:
        existing=conn.execute('SELECT partition, offset, body FROM records').fetchall()
        conn.execute('CREATE TABLE records_v2(partition TEXT NOT NULL, offset INTEGER NOT NULL, '
                     'digest TEXT NOT NULL, event_id TEXT, body TEXT, PRIMARY KEY(partition,offset))')
        for partition,offset,body in existing:
            record=loads(body)
            conn.execute('INSERT INTO records_v2 VALUES (?,?,?,?,?)',
                         (partition,offset,_digest(body),record.get('id'),body))
        conn.execute('DROP TABLE records')
        conn.execute('ALTER TABLE records_v2 RENAME TO records')
        conn.execute('CREATE TABLE tombstones(id TEXT PRIMARY KEY, erased_at INTEGER NOT NULL)')
        conn.execute("INSERT OR REPLACE INTO meta VALUES ('erase_epoch','0')")
        conn.execute("UPDATE meta SET value=? WHERE key='schema'",(json.dumps(SCHEMA_VERSION),))
        conn.execute('COMMIT')
    except BaseException:
        conn.execute('ROLLBACK');raise

def init(db,partitions,width,delay):
    if not partitions:raise ValueError('expected at least one partition')
    for name in partitions:ident(name)
    if len(set(partitions))!=len(partitions):raise ValueError('duplicate partition name')
    number(width,1,86400);number(delay,0,86400)
    dest=Path(db)
    if dest.exists():raise ValueError('state already exists: '+str(dest))
    dest.parent.mkdir(parents=True,exist_ok=True)
    conn=sqlite3.connect(dest)
    try:
        conn.executescript(SCHEMA)
        for key,value in [('schema',SCHEMA_VERSION),('window_seconds',width),('out_of_order_seconds',delay),
                          ('watermark',None),('duplicates',0),('erase_epoch',0)]:
            conn.execute('INSERT INTO meta VALUES (?,?)',(key,json.dumps(value)))
        for name in partitions:
            conn.execute('INSERT INTO partitions VALUES (?,?,0,NULL)',(name,'active'))
        conn.commit()
    finally:conn.close()

def connect(db):
    if not Path(db).exists():raise ValueError('state not initialized: '+str(db))
    conn=sqlite3.connect(db,isolation_level=None)
    try:
        try:schema=_meta(conn,'schema')
        except (ValueError,sqlite3.Error):
            conn.close();raise ValueError('not a stream state database: '+str(db))
        if schema==1:_migrate(conn);schema=_meta(conn,'schema')
        if schema!=SCHEMA_VERSION:conn.close();raise ValueError('unsupported state schema: '+str(db))
        return conn
    except BaseException:
        conn.close();raise

def _snapshot(conn):
    """Read the erase epoch and the full report from one consistent read snapshot."""
    conn.execute('BEGIN')
    try:
        epoch=_meta(conn,'erase_epoch');value=report_from(conn);conn.execute('COMMIT')
    except BaseException:
        conn.execute('ROLLBACK');raise
    return epoch,value

def apply(db,batch):
    return _apply(db,batch)[1]

def _apply(db,batch):
    lines=_read_batch(batch)
    with state_lock([db],exclusive=True):
        conn=connect(db)
        try:
            known={r[0] for r in conn.execute('SELECT name FROM partitions')}
            conn.execute('BEGIN IMMEDIATE')
            try:
                for line in lines:_apply_record(conn,_validated(loads(line)),known)
                conn.execute('COMMIT')
            except BaseException:
                conn.execute('ROLLBACK');raise
            return _snapshot(conn)
        finally:conn.close()

def _validated(record):
    _validate_record(record);return record

def report_from(conn):
    width,delay=_params(conn)
    def window_row(start,count,total,closed_by=None):
        ids=[r[0] for r in conn.execute('SELECT id FROM contributions WHERE start=? ORDER BY seq',(start,))]
        row={'start':start,'end':start+width,'count':count,'sum':total,'event_ids':ids}
        if closed_by:row['closed_by']=closed_by
        return row
    closed=[window_row(s,c,t,'watermark') for s,c,t in
            conn.execute('SELECT start,count,total FROM windows WHERE closed=1 ORDER BY close_seq')]
    pending=[window_row(s,c,t) for s,c,t in
             conn.execute('SELECT start,count,total FROM windows WHERE closed=0 ORDER BY start')]
    late=[{'id':i,'event_time':t,'value':v,'partition':p,'reason':'window_already_closed','watermark_at_arrival':w}
          for i,t,v,p,w in conn.execute('SELECT id,event_time,value,partition,watermark_at_arrival FROM late_events ORDER BY seq')]
    partitions={}
    for name,status,next_offset,maximum in conn.execute('SELECT name,status,next_offset,max_event_time FROM partitions ORDER BY name'):
        partitions[name]={'status':status,'next_offset':next_offset,'max_event_time':maximum,
                          'watermark':None if maximum is None else maximum-delay}
    accepted=conn.execute('SELECT COALESCE(SUM(count),0), COALESCE(SUM(total),0) FROM windows').fetchone()
    maximum=conn.execute('SELECT MAX(max_event_time) FROM partitions').fetchone()[0]
    return {'schema_version':1,'window_seconds':width,'out_of_order_seconds':delay,
            'watermark':_meta(conn,'watermark'),'max_event_time':maximum,
            'erase_epoch':_meta(conn,'erase_epoch'),
            'erased_event_ids':[r[0] for r in conn.execute('SELECT id FROM tombstones ORDER BY id')],
            'closed_windows':closed,'pending_windows':pending,'late_events':late,
            'duplicate_events':_meta(conn,'duplicates'),
            'accepted_count':accepted[0],'accepted_sum':accepted[1],'partitions':partitions}

def report(db):
    return _report(db)[1]

def _report(db):
    with state_lock([db],exclusive=True):  # the exclusive lock also serialises a schema 1 migration
        conn=connect(db)
        try:return _snapshot(conn)
        finally:conn.close()

def _load_ids(path):
    data,_=read(path)
    require(data,'event_ids')
    values=rows(data['event_ids'],20000)
    for eid in values:ident(eid)
    return values

def erase(db,ids):
    """Erase event payloads by id; return the report published from the committed state."""
    return _erase(db,ids)[1]

def _erase(db,ids):
    unique=[]
    for eid in ids:
        ident(eid)
        if eid not in unique:unique.append(eid)
    if len(unique)>20000:raise ValueError('erase exceeds 20000 event ids')
    with state_lock([db],exclusive=True):
        conn=connect(db)
        try:
            conn.execute('BEGIN IMMEDIATE')
            try:
                epoch=_meta(conn,'erase_epoch');changed=False
                for eid in unique:
                    if conn.execute('SELECT 1 FROM tombstones WHERE id=?',(eid,)).fetchone():
                        continue  # repeated erase of an already erased id: idempotent
                    row=conn.execute('SELECT event_time, value FROM seen WHERE id=?',(eid,)).fetchone()
                    if row is None:raise ValueError('unknown event ID: '+eid)
                    value=row[1]
                    starts=[r[0] for r in conn.execute('SELECT start FROM contributions WHERE id=?',(eid,)).fetchall()]
                    for start in starts:
                        conn.execute('UPDATE windows SET count=count-1, total=total-? WHERE start=?',(value,start))
                    conn.execute('DELETE FROM contributions WHERE id=?',(eid,))
                    conn.execute('DELETE FROM late_events WHERE id=?',(eid,))
                    conn.execute('DELETE FROM seen WHERE id=?',(eid,))
                    conn.execute('UPDATE records SET body=NULL, event_id=NULL WHERE event_id=?',(eid,))
                    conn.execute('INSERT INTO tombstones VALUES (?,?)',(eid,epoch+1))
                    changed=True
                if changed:_set_meta(conn,'erase_epoch',epoch+1)
                conn.execute('COMMIT')
            except BaseException:
                conn.execute('ROLLBACK');raise
            return _snapshot(conn)
        finally:conn.close()

def _check_output(db,output,batch=None):
    out=Path(output).resolve()
    if out==Path(db).resolve():raise ValueError('output would overwrite state database')
    if batch is not None and out==Path(batch).resolve():raise ValueError('input and output must differ')

def _publish(db,epoch,output,result):
    gate('snapshot-taken')
    with publication_guard({db:epoch}):
        gate('validated')
        save(output,result)
        gate('saved')
        print(json.dumps(result,sort_keys=True,ensure_ascii=False,allow_nan=False),flush=True)

def main(argv=None):
    parser=argparse.ArgumentParser(prog='watermark-windows stream',description=__doc__.splitlines()[0])
    sub=parser.add_subparsers(dest='command',required=True)
    p_init=sub.add_parser('init',help='create state: fixed partitions, window width, delay')
    p_init.add_argument('--db',type=Path,required=True)
    p_init.add_argument('--partitions',required=True,help='comma-separated partition names')
    p_init.add_argument('--window-seconds',type=int,required=True)
    p_init.add_argument('--out-of-order-seconds',type=int,required=True)
    p_apply=sub.add_parser('apply',help='apply one JSONL batch atomically, then publish report')
    p_apply.add_argument('--db',type=Path,required=True)
    p_apply.add_argument('--batch',type=Path,required=True)
    p_apply.add_argument('--output',type=Path,required=True)
    p_report=sub.add_parser('report',help='republish the full report from state alone')
    p_report.add_argument('--db',type=Path,required=True)
    p_report.add_argument('--output',type=Path,required=True)
    p_erase=sub.add_parser('erase',help='erase event payloads by id atomically, then publish report')
    p_erase.add_argument('--db',type=Path,required=True)
    p_erase.add_argument('--ids',type=Path,required=True,help='JSON file: {"event_ids": [...]}')
    p_erase.add_argument('--output',type=Path,required=True)
    args=parser.parse_args(argv)
    try:
        if args.command=='init':
            init(args.db,args.partitions.split(','),args.window_seconds,args.out_of_order_seconds);return 0
        if args.command=='apply':
            _check_output(args.db,args.output,args.batch)
            epoch,result=_apply(args.db,args.batch)
        elif args.command=='erase':
            _check_output(args.db,args.output,args.ids)
            epoch,result=_erase(args.db,_load_ids(args.ids))
        else:
            _check_output(args.db,args.output)
            epoch,result=_report(args.db)
        _publish(args.db,epoch,args.output,result);return 0
    except (ValueError,TypeError,KeyError,OSError,OverflowError,RecursionError,sqlite3.Error) as error:
        print(str(error),file=sys.stderr);return 2

if __name__=='__main__':raise SystemExit(main())
