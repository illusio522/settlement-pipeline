"""Freeze a real CDC landing and a file bundle, then run both engines independently."""
from pathlib import Path
import shutil
import uuid
import duckdb
import pyarrow.parquet as pq

from .bundle import BatchError, SCHEMAS, atomic_json, read_manifest, sha256, snapshot
from .compare import compare
from .compute import compute
from .reference import legacy_from_landing


def verify_inputs(legacy_db,file_db,manifest,report):
    """Equivalent output alone isn't proof of identical input. Compare the selected input scope too."""
    day=manifest['settlement_date']
    scopes={
        'payment':f"SELECT * FROM old.payment WHERE CAST(event_time AS DATE)=DATE '{day}'",
        'payment_method':f"SELECT * FROM old.payment_method WHERE txn_id IN (SELECT txn_id FROM old.payment WHERE CAST(event_time AS DATE)=DATE '{day}')",
        'original_payment':'SELECT * FROM old.payment WHERE txn_id IN (SELECT orig_txn_id FROM new.raw.payment WHERE type=2)',
        'original_payment_method':'SELECT * FROM old.payment_method WHERE txn_id IN (SELECT orig_txn_id FROM new.raw.payment WHERE type=2)',
        'order_item':'SELECT * FROM old.order_item WHERE order_no IN (SELECT order_no FROM new.raw.payment)',
        'cancel':'SELECT * FROM old.cancel WHERE cancel_txn_id IN (SELECT txn_id FROM new.raw.payment WHERE type=2)',
        **{t:f'SELECT * FROM old.{t}' for t in ('partner','method_fee','mid_map')}}
    results={}
    with duckdb.connect() as db:
        for alias,path in (('old',legacy_db),('new',file_db)):
            escaped=str(Path(path).resolve()).replace("'","''")
            db.execute(f"ATTACH '{escaped}' AS {alias} (READ_ONLY)")
        for name,scope in scopes.items():
            cols=','.join(SCHEMAS[name].names)
            a=f'SELECT {cols} FROM ({scope})'
            b=f'SELECT {cols} FROM new.raw.{name}'
            missing=db.execute(f'SELECT COUNT(*) FROM ({a} EXCEPT ALL {b})').fetchone()[0]
            extra=db.execute(f'SELECT COUNT(*) FROM ({b} EXCEPT ALL {a})').fetchone()[0]
            results[name]={'legacy_only':missing,'file_only':extra,'matched':missing==extra==0}
    atomic_json(report,results)
    if not all(v['matched'] for v in results.values()):
        raise BatchError(f'두 경로의 원천 입력이 다릅니다: {report}')
    return results


def verify(bundle,landing,work_root):
    manifest,digest=read_manifest(bundle)
    work=Path(work_root).resolve()/str(uuid.uuid4())
    work.mkdir(parents=True)
    frozen=snapshot(bundle,work/'file-input',manifest)
    # Caller must stop the consumer / use a completed landing. Freeze content and detect writes.
    legacy_input=work/'cdc-input'; legacy_input.mkdir()
    capture=[]
    for table in ('payment','payment_method','order_item','cancel'):
        paths=sorted(Path(landing).glob(f'{table}.p*.ndjson'))
        if not paths:
            raise BatchError(f'CDC 랜딩 누락: {table}')
        for src in paths:
            before=sha256(src)
            dst=legacy_input/src.name
            shutil.copyfile(src,dst)
            if before!=sha256(dst) or before!=sha256(src):
                raise BatchError('소비 중 랜딩은 비교할 수 없습니다. 컨슈머를 종료한 뒤 다시 실행하세요')
            capture.append({'file':src.name,'sha256':before})
    atomic_json(work/'cdc-capture.json',capture)
    masters={name:[row for e in manifest['tables'][name]
                   for row in pq.read_table(frozen/e['path']).to_pylist()]
             for name in ('partner','method_fee','mid_map')}
    reference=legacy_from_landing(legacy_input,work/'legacy.duckdb',masters)
    file_work=work/'file-result';file_work.mkdir()
    result=compute(frozen,manifest,file_work)
    verify_inputs(reference,result,manifest,work/'input-comparison.json')
    report=compare(reference,result,manifest['settlement_date'],work/'result-comparison.json')
    summary=dict(matched=True,work=str(work),manifest_hash=digest,
                 legacy_db=str(reference),file_db=str(result),tables=report['tables'])
    atomic_json(work/'summary.json',summary)
    return summary
