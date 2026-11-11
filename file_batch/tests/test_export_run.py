import json
from pathlib import Path

import duckdb
import psycopg
import pytest

from file_batch.bundle import BatchError, read_manifest
from file_batch.export import export_bundle
from file_batch.runner import run
from file_batch.compare import compare
from file_batch.reference import legacy_from_landing
from .fixtures import DAY, data, close_for, master_json
from .test_bundle_compute import landing


def seed(dsns,source):
    with psycopg.connect(dsns[0]) as conn:
        conn.execute('TRUNCATE payment,payment_card,payment_money,payment_point,payment_coupon')
        for row in source['payment']:
            cols=list(row)
            conn.execute(f'INSERT INTO payment({",".join(cols)}) VALUES({",".join("%s" for _ in cols)})',list(row.values()))
        for row in source['payment_method']:
            r=dict(row);method=r.pop('method')
            if method>2: r.pop('payments_mid')
            name=['card','money','point','coupon'][method-1]
            conn.execute(f'INSERT INTO payment_{name}({",".join(r)}) VALUES({",".join("%s" for _ in r)})',list(r.values()))
    with psycopg.connect(dsns[1]) as conn:
        conn.execute('TRUNCATE order_item,cancel')
        for row in source['order_item']:
            r=dict(row,product_id=row['order_no'],qty=1,unit_price=row['item_amount'])
            conn.execute(f'INSERT INTO order_item({",".join(r)}) VALUES({",".join("%s" for _ in r)})',list(r.values()))
        for r in source['cancel']:
            conn.execute(f'INSERT INTO cancel({",".join(r)}) VALUES({",".join("%s" for _ in r)})',list(r.values()))


def test_postgres_export_compute_publish_and_equivalence(store,source_dsns,tmp_path):
    source=data()
    seed(source_dsns,source)
    master=tmp_path/'masters.json'; master_json(master,source)
    close=tmp_path/'close.json'; close.write_text(json.dumps(close_for(source)))
    b=export_bundle(*source_dsns,DAY,tmp_path/'bundles',master,close)
    manifest,_=read_manifest(b)
    assert manifest['tables']['original_payment'][0]['rows']==1
    result=run(b,tmp_path/'runs',store.dsn)
    assert result['revision']==1
    repeat=run(b,tmp_path/'runs',store.dsn)
    assert repeat['reused'] and repeat['run_id']==result['run_id']
    # Both paths start from the same shared source fixture; legacy loader includes duplicates.
    legacy=legacy_from_landing(landing(tmp_path/'landing',source),tmp_path/'legacy.duckdb',
                               {t:source[t] for t in ('partner','method_fee','mid_map')})
    assert compare(legacy,result['database'],DAY,tmp_path/'equivalence.json')['matched']
    with store.connect() as conn:
        assert conn.execute('SELECT SUM(gross) AS gross,SUM(cancel) AS cancel FROM settlement_agg').fetchone()==dict(gross=101,cancel=33)
    # Source changes after extraction must not change an existing bundle's result.
    with psycopg.connect(source_dsns[0]) as conn:
        conn.execute('UPDATE payment SET amount=999 WHERE txn_id=200')
    assert run(b,tmp_path/'runs',store.dsn)['reused']


def test_failed_quality_gate_does_not_publish(store,source_dsns,tmp_path):
    source=data(); source['payment_method'][2]['amount']+=1
    seed(source_dsns,source)
    master=tmp_path/'masters.json'; master_json(master,source)
    close=tmp_path/'close.json'; close.write_text(json.dumps(close_for(source)))
    b=export_bundle(*source_dsns,DAY,tmp_path/'bundles',master,close)
    with pytest.raises(BatchError): run(b,tmp_path/'runs',store.dsn)
    with store.connect() as conn:
        assert conn.execute('SELECT COUNT(*) AS n FROM settlement_agg').fetchone()['n']==0
        assert conn.execute('SELECT status FROM settlement_run').fetchone()['status']=='FAILED'
