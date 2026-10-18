import hashlib
from pathlib import Path
import threading
import time

import duckdb

from .bundle import BatchError, atomic_json, json_bytes, read_manifest, snapshot
from .compute import AGG_COLUMNS, POLICY, code_version, compute
from .store import Store


def run(bundle, work_root, dsn, expected_revision=0):
    manifest, digest = read_manifest(bundle)
    version = code_version()
    key = hashlib.sha256(json_bytes([manifest['settlement_date'],digest,version,POLICY])).hexdigest()
    store = Store(dsn)
    row = store.claim(key, manifest, digest, version, expected_revision)
    if row['status'] == 'PUBLISHED':
        return dict(run_id=str(row['run_id']),status='PUBLISHED',reused=True)
    run_id, token = row['run_id'], row['owner_token']
    work = Path(work_root).resolve() / str(run_id) / f'attempt={row["attempt"]}'
    work.mkdir(parents=True, exist_ok=False)
    stop = threading.Event()
    errors = []
    def heartbeat():
        while not stop.wait(5):
            try:
                store.heartbeat(run_id,token)
            except Exception as exc:
                errors.append(exc)
                return
    thread = threading.Thread(target=heartbeat, daemon=True)
    thread.start()
    started = time.monotonic()
    try:
        frozen = snapshot(bundle, work / 'input', manifest)
        dbpath = compute(frozen,manifest,work)
        if errors:
            raise BatchError('heartbeat 실패; 게시 중단') from errors[0]
        with duckdb.connect(str(dbpath), read_only=True) as db:
            rows = db.execute('SELECT '+','.join(AGG_COLUMNS)+' FROM settlement_agg ORDER BY ALL').fetchall()
        store.stage(run_id,token,rows)
        revision = store.publish(run_id,token)
        result = dict(run_id=str(run_id),status='PUBLISHED',revision=revision,reused=False,
                      seconds=round(time.monotonic()-started,3),database=str(dbpath))
        atomic_json(work / 'result.json', result)
        return result
    except Exception as exc:
        store.fail(run_id,token,str(exc))
        atomic_json(work / 'failure.json', dict(error=str(exc),seconds=round(time.monotonic()-started,3)))
        raise
    finally:
        stop.set()
        thread.join(timeout=10)
