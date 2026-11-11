from copy import deepcopy
import json
from pathlib import Path

import duckdb
import pytest

from file_batch.bundle import BatchError, read_manifest, snapshot
from file_batch.compute import compute, input_checks, load_inputs, Gates
from file_batch.compare import compare
from file_batch.reference import legacy_from_landing
from .fixtures import DAY, bundle, data


def calculate(root,source=None,day=DAY):
    b=bundle(root/'bundles',source,day)
    m,_=read_manifest(b)
    work=root/'work'
    work.mkdir(parents=True)
    snap=snapshot(b,work/'input',m)
    return compute(snap,m,work),b


def landing(root,source,duplicates=True):
    root.mkdir()
    for name in ('payment','payment_method','order_item','cancel'):
        lines=[]
        for i,row in enumerate(source[name]):
            r=dict(row)
            if 'event_time' in r:
                # Existing CDC schema uses epoch milliseconds for local wall-clock timestamp.
                from datetime import datetime
                r['event_time']=int((r['event_time']-datetime(1970,1,1)).total_seconds()*1000)
            lines.append(dict(_op='c',_seq=i+1,**r))
        if duplicates:
            lines=lines+lines
        (root/f'{name}.p0.ndjson').write_text('\n'.join(json.dumps(r) for r in lines)+'\n')
    return root


@pytest.mark.parametrize('day',['2026-05-19',DAY,'2026-05-21'])
def test_cdc_and_file_full_row_equivalence(tmp_path,day):
    source=data()
    file_db,_=calculate(tmp_path/'file',source,day)
    raw=landing(tmp_path/'landing',source)
    legacy=legacy_from_landing(raw,tmp_path/'legacy.duckdb',
                               {t:source[t] for t in ('partner','method_fee','mid_map')})
    result=compare(legacy,file_db,day,tmp_path/'comparison.json')
    assert result['matched']
    with duckdb.connect(str(file_db)) as db:
        if day==DAY:
            assert db.execute('SELECT SUM(gross),SUM(cancel) FROM settlement_agg').fetchone()==(101,33)
            assert db.execute('SELECT COUNT(*) FROM settlement_event WHERE txn_id=100').fetchone()[0]==0


def test_comparator_detects_value_difference_with_equal_row_count(tmp_path):
    file_db,_=calculate(tmp_path/'file')
    source=data()
    ref=legacy_from_landing(landing(tmp_path/'landing',source),tmp_path/'ref.duckdb',
                            {t:source[t] for t in ('partner','method_fee','mid_map')})
    with duckdb.connect(str(ref)) as db:
        db.execute('UPDATE settlement_event SET fee=fee+1 WHERE txn_id=200')
    with pytest.raises(BatchError,match='불일치'):
        compare(ref,file_db,DAY,tmp_path/'diff.json')
    report=json.loads((tmp_path/'diff.json').read_text())
    assert report['tables']['settlement_agg']['matched']
    assert not report['tables']['settlement_event']['matched']


@pytest.mark.parametrize('failure',['missing','corrupt','traversal','duplicate_path','missing_table','schema'])
def test_bundle_rejects_invalid_inputs(tmp_path,failure):
    b=bundle(tmp_path)
    m,_=read_manifest(b)
    entry=m['tables']['payment'][0]
    f=b/entry['path']
    if failure=='missing': f.unlink()
    if failure=='corrupt': f.write_bytes(b'not parquet')
    if failure=='traversal': entry['path']='../outside.parquet'
    if failure=='duplicate_path': m['tables']['payment'].append(entry.copy())
    if failure=='missing_table': del m['tables']['cancel']
    if failure=='schema': m['schema_version']=99
    (b/'manifest.json').write_text(json.dumps(m))
    with pytest.raises((BatchError,FileNotFoundError)):
        m,_=read_manifest(b)
        snapshot(b,tmp_path/'snapshot',m)


@pytest.mark.parametrize('failure',['method_sum','duplicate_key','orphan_partner','missing_original','missing_fee','missing_cancel','close_total','wrong_mid','unsupported_type'])
def test_input_quality_gates(tmp_path,failure):
    source=data()
    if failure=='method_sum': source['payment_method'][2]['amount']+=1
    if failure=='duplicate_key': source['payment_method'].append(source['payment_method'][2].copy())
    if failure=='orphan_partner': source['partner'].pop()
    if failure=='missing_original': source['payment']=[p for p in source['payment'] if p['txn_id']!=100]
    if failure=='missing_fee': source['method_fee']=[f for f in source['method_fee'] if f['method']!=1]
    if failure=='missing_cancel': source['cancel']=[]
    if failure=='wrong_mid': source['payment_method'][2]['payments_mid']=9999
    if failure=='unsupported_type': source['payment'][1]['type']=7
    b=bundle(tmp_path,source)
    m,_=read_manifest(b)
    if failure=='close_total': m['close']['expected']['gross']+=1
    with duckdb.connect() as db:
        load_inputs(db,b,m)
        with pytest.raises(BatchError):
            input_checks(db,m,Gates(db,tmp_path/'checks.json'))


def test_same_order_repayment_is_rejected_until_both_engines_support_it(tmp_path):
    source=data()
    source['payment'][1]['order_no']=10
    source['payment_method'][2]['order_no']=10
    source['payment_method'][3]['order_no']=10
    b=bundle(tmp_path,source)
    m,_=read_manifest(b)
    with duckdb.connect() as db:
        load_inputs(db,b,m)
        with pytest.raises(BatchError,match='single_original'):
            input_checks(db,m,Gates(db,tmp_path/'checks.json'))

@pytest.mark.parametrize('day',['2026-05-15','2026-06-02','2026-06-08'])
def test_original_generator_regression_1000_orders(tmp_path,day):
    # Independent existing generator and SQL are the regression oracle.
    from file_batch.reference import REPO,sql_script
    with duckdb.connect() as db:
        db.execute(sql_script(REPO/'transform/01_schema.sql'))
        seed=sql_script(REPO/'generator/02_seed.sql').replace('generate_series(1,100000)', 'generate_series(1,1000)')
        db.execute(seed)
        source={}
        for table in ('payment','payment_method','order_item','cancel','partner','method_fee','mid_map'):
            cur=db.execute('SELECT * FROM '+table)
            columns=[d[0] for d in cur.description]
            source[table]=[dict(zip(columns,row)) for row in cur.fetchall()]
    file_db,_=calculate(tmp_path/'file',source,day)
    legacy=legacy_from_landing(landing(tmp_path/'landing',source),tmp_path/'legacy.duckdb',
                               {t:source[t] for t in ('partner','method_fee','mid_map')})
    assert compare(legacy,file_db,day,tmp_path/'comparison.json')['matched']


def test_rounding_boundary_matches_legacy(tmp_path):
    from datetime import datetime
    from decimal import Decimal
    source=data()
    source['payment']=source['payment'][1:2]
    source['payment'][0]['amount']=100
    source['payment_method']=source['payment_method'][2:3]
    source['payment_method'][0]['amount']=100
    source['order_item']=[dict(order_no=20,line_no=i,partner_id=1,brand=1,item_amount=1) for i in (1,2,3)]
    source['cancel']=[]
    # 100/3 -> 33,33,34. Fee on 33 at 50% -> HALF_UP 17,17,17.
    for fee in source['method_fee']:
        if fee['method']==1: fee['fee_rate']=Decimal('0.5000')
    file_db,_=calculate(tmp_path/'file',source)
    # Legacy loader needs an explicit deleted cancel marker when there are zero cancel rows.
    raw=landing(tmp_path/'landing',source)
    (raw/'cancel.p0.ndjson').write_text(json.dumps(dict(_op='d',_seq=1,cancel_txn_id=999,order_no=999,line_no=1)))
    legacy=legacy_from_landing(raw,tmp_path/'legacy.duckdb',{t:source[t] for t in ('partner','method_fee','mid_map')})
    assert compare(legacy,file_db,DAY,tmp_path/'comparison.json')['matched']
    with duckdb.connect(str(file_db)) as db:
        assert db.execute('SELECT allocated_amount,fee FROM settlement_event ORDER BY line_no').fetchall()==[(33,17),(33,17),(34,17)]
