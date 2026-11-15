"""Strict local JSON contract and atomic report publication."""
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import quote
import fcntl, hashlib, json, os, sqlite3, tempfile, time

class EpochChanged(ValueError):
    """The erase epoch changed between snapshot and publication."""

def gate(name):
    """Test-only rendezvous point; inert unless WATERMARK_GATE_DIR is set.

    The process announces arrival by creating <name>.reached.  It then blocks
    until <name>.release appears, but only for names listed in the optional
    comma-separated WATERMARK_GATE_BLOCK (default: block on every gate).
    """
    directory=os.environ.get('WATERMARK_GATE_DIR')
    if not directory:return
    root=Path(directory)
    (root/(name+'.reached')).write_text('1')
    block=os.environ.get('WATERMARK_GATE_BLOCK')
    if block is not None and name not in block.split(','):return
    release=root/(name+'.release')
    while not release.exists():time.sleep(0.005)

def require(obj,keys):
    if not isinstance(obj,dict) or set(obj)!=set(keys.split()):raise ValueError('expected object fields: '+keys)

def ident(v):
    if not isinstance(v,str) or not v or v!=v.strip() or len(v)>128 or any(ord(c)<32 for c in v):
        raise ValueError('expected nonblank identifier, <=128 characters')
    return v

def number(v,minimum=0,maximum=10**9):
    if type(v) is not int or not minimum<=v<=maximum:raise ValueError('integer outside allowed bounds')
    return v

def rows(value,limit=20000):
    if not isinstance(value,list) or len(value)>limit:raise ValueError('expected bounded array')
    return value

def _unique(pairs):
    result={}
    for k,v in pairs:
        if k in result:raise ValueError('duplicate JSON key: '+k)
        result[k]=v
    return result

def _invalid(v):raise ValueError('nonfinite JSON number: '+v)

def loads(raw):
    return json.loads(raw,object_pairs_hook=_unique,parse_constant=_invalid)

def read(path):
    with Path(path).open('rb') as stream:raw=stream.read(8_000_001)
    if len(raw)>8_000_000:raise ValueError('JSON exceeds 8 MB')
    return loads(raw),hashlib.sha256(raw).hexdigest()

def save(path,report):
    dest=Path(path)
    if dest.is_symlink():raise ValueError('output cannot be a symlink')
    dest.parent.mkdir(parents=True,exist_ok=True);temp=None
    try:
        with tempfile.NamedTemporaryFile(mode='w',encoding='utf-8',dir=dest.parent,delete=False) as stream:
            temp=stream.name;json.dump(report,stream,sort_keys=True,ensure_ascii=False,allow_nan=False,indent=2);stream.write('\n')
        os.replace(temp,dest)
    finally:
        if temp and os.path.exists(temp):os.unlink(temp)

def _read_only(path):
    return sqlite3.connect('file:%s?mode=ro'%quote(str(Path(path).resolve()),safe='/'),uri=True,isolation_level=None)

def read_epoch(path):
    """Erase epoch of one state database; schema 1 states have no epoch and report 0."""
    p=Path(path)
    if not p.exists():raise ValueError('state not initialized: '+str(p))
    try:conn=_read_only(p)
    except sqlite3.Error as error:raise ValueError(str(error))
    try:
        try:row=conn.execute("SELECT value FROM meta WHERE key='erase_epoch'").fetchone()
        except sqlite3.Error:raise ValueError('not a stream state database: '+str(p))
        return 0 if row is None else int(json.loads(row[0]))
    finally:conn.close()

def _lock_fds(paths,exclusive):
    """Open and flock each distinct resolved path, in sorted order (deadlock-free)."""
    fds=[]
    try:
        for p in sorted({str(Path(q).resolve()) for q in paths}):
            try:fd=os.open(p,os.O_RDONLY)
            except OSError as error:raise ValueError('cannot lock state: '+str(error))
            try:fcntl.flock(fd,fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
            except OSError:
                os.close(fd);raise
            fds.append(fd)
    except BaseException:
        for fd in fds:
            try:fcntl.flock(fd,fcntl.LOCK_UN)
            finally:os.close(fd)
        raise
    return fds

def _unlock_fds(fds):
    for fd in fds:
        try:fcntl.flock(fd,fcntl.LOCK_UN)
        finally:os.close(fd)

@contextmanager
def state_lock(paths,exclusive=False):
    """Advisory whole-file lock on every distinct resolved state path.

    Writers (apply, erase) take exclusive locks; readers that publish take
    shared locks.  Locking the resolved database file means path aliases
    (relative paths, symlinks, hard links) contend on the same inode.
    """
    fds=_lock_fds(paths,exclusive)
    try:yield
    finally:_unlock_fds(fds)

@contextmanager
def publication_guard(epochs):
    """Hold shared state locks and re-verify erase epochs before publishing.

    `epochs` is a mapping or an iterable of (state path, epoch) pairs observed in
    the snapshots the report was computed from.  The same resolved state may
    appear twice (e.g. one database used on both sides of a reconciliation): both
    observations must agree, otherwise the report is stale across its own reads.
    Shared locks are held while the caller replaces the report file and writes
    stdout, so an erase (which needs the exclusive lock) cannot commit in that
    window.  A changed epoch raises EpochChanged: the caller must leave the old
    report in place and exit 2.
    """
    pairs=epochs.items() if hasattr(epochs,'items') else epochs
    expected={}
    for path,epoch in pairs:
        key=str(Path(path).resolve())
        if key in expected and expected[key]!=int(epoch):
            raise EpochChanged('erase epoch changed between snapshots; report not published')
        expected[key]=int(epoch)
    with state_lock(list(expected)):
        for path,epoch in expected.items():
            if read_epoch(path)!=epoch:
                raise EpochChanged('erase epoch changed; report not published')
        yield
