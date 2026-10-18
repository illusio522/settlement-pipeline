"""Input gates, isolated dbt execution and output reconciliation."""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import duckdb

from .bundle import BatchError, SCHEMAS, atomic_json, json_bytes

ROOT = Path(__file__).resolve().parent
POLICY = 'krw-line-cancel-pay-fee-v1'
AGG_COLUMNS = 'settlement_date partner_id brand method gross cancel net fee payout txn_count'.split()
EVENT_COLUMNS = ('event_id txn_id order_no line_no partner_id brand type method payments_mid '
                 'allocated_amount fee_rate fee net_amount event_time settlement_date').split()


def code_version():
    h = hashlib.sha256()
    for p in sorted(ROOT.rglob('*')):
        if p.is_file() and p.name != '.user.yml' and p.suffix in ('.py', '.sql', '.yml', '.txt', '.lock') and not any(
            x in p.parts for x in ('tests', 'target', 'logs', '__pycache__')):
            h.update(str(p.relative_to(ROOT)).encode())
            h.update(p.read_bytes())
    return h.hexdigest()


def load_inputs(db, bundle, manifest):
    db.execute('CREATE SCHEMA raw')
    for name, entries in manifest['tables'].items():
        files = [str(Path(bundle) / e['path']) for e in entries]
        db.from_parquet(files, union_by_name=False).create_view('_batch_input')
        db.execute(f'CREATE TABLE raw.{name} AS SELECT * FROM _batch_input')
        db.execute('DROP VIEW _batch_input')


class Gates:
    def __init__(self, db, report):
        self.db, self.report, self.results = db, Path(report), []

    def check(self, name, query, params=None):
        rows = self.db.execute(query, params or []).fetchmany(5)
        self.results.append(dict(name=name, passed=not rows,
                                 examples=[[str(v) for v in row] for row in rows]))
        atomic_json(self.report, self.results)
        if rows:
            raise BatchError(f'정합성 검사 실패: {name}')


def input_checks(db, manifest, gates):
    day = manifest['settlement_date']
    keys = {'payment':'txn_id', 'original_payment':'txn_id',
            'payment_method':'method,pm_id', 'original_payment_method':'method,pm_id',
            'order_item':'order_no,line_no', 'cancel':'cancel_txn_id,order_no,line_no',
            'partner':'partner_id', 'method_fee':'method,effective_from', 'mid_map':'payments_mid'}
    for name, spec in SCHEMAS.items():
        nulls = ' OR '.join(f'{f.name} IS NULL' for f in spec if not f.nullable)
        gates.check(f'{name}:required', f'SELECT * FROM raw.{name} WHERE {nulls}')
        gates.check(f'{name}:unique', f'SELECT {keys[name]} FROM raw.{name} GROUP BY {keys[name]} HAVING COUNT(*)>1')
    gates.check('target_date', 'SELECT txn_id FROM raw.payment WHERE CAST(event_time AS DATE) <> ?::DATE', [day])
    for table in ('payment', 'original_payment'):
        gates.check(f'{table}:amount_type', f'SELECT txn_id FROM raw.{table} WHERE amount<=0 '
                    'OR amount>1000000000000 OR type NOT IN (1,2)')
    gates.check('original_pay_only', 'SELECT txn_id FROM raw.original_payment WHERE type<>1 OR orig_txn_id IS NOT NULL')
    gates.check('target_original_link', '''SELECT p.txn_id FROM raw.payment p LEFT JOIN raw.original_payment o ON o.txn_id=p.orig_txn_id
        WHERE (p.type=1 AND p.orig_txn_id IS NOT NULL) OR (p.type=2 AND
        (o.txn_id IS NULL OR o.order_no<>p.order_no OR o.brand<>p.brand OR p.amount>o.amount))''')
    gates.check('item_amount', 'SELECT order_no FROM raw.order_item WHERE item_amount<=0 OR item_amount>1000000000000')
    gates.check('partner_link', '''SELECT oi.order_no FROM raw.order_item oi LEFT JOIN raw.partner p USING(partner_id)
        WHERE p.partner_id IS NULL OR p.brand<>oi.brand''')
    gates.check('order_link', '''SELECT p.txn_id FROM raw.payment p LEFT JOIN raw.order_item oi USING(order_no)
        GROUP BY p.txn_id,p.brand HAVING COUNT(oi.line_no)=0 OR MIN(oi.brand)<>p.brand OR MAX(oi.brand)<>p.brand''')
    gates.check('cancel_link', '''SELECT c.cancel_txn_id FROM raw.cancel c LEFT JOIN raw.payment p ON p.txn_id=c.cancel_txn_id
        LEFT JOIN raw.order_item oi ON oi.order_no=c.order_no AND oi.line_no=c.line_no
        WHERE p.txn_id IS NULL OR p.type<>2 OR p.order_no<>c.order_no OR oi.line_no IS NULL''')
    gates.check('cancel_line_total', '''SELECT p.txn_id FROM raw.payment p LEFT JOIN raw.cancel c ON c.cancel_txn_id=p.txn_id
        LEFT JOIN raw.order_item oi ON oi.order_no=c.order_no AND oi.line_no=c.line_no
        WHERE p.type=2 GROUP BY p.txn_id,p.amount HAVING COUNT(c.line_no)=0 OR SUM(oi.item_amount)<>p.amount''')
    gates.check('duplicate_cancel_line', '''SELECT p.orig_txn_id,c.line_no FROM raw.payment p JOIN raw.cancel c ON c.cancel_txn_id=p.txn_id
        GROUP BY p.orig_txn_id,c.line_no HAVING COUNT(*)>1''')
    # The first version supports the existing domain: one original PAY per order, full item-line cancels.
    gates.check('single_original_per_order', '''SELECT order_no FROM (
        SELECT txn_id,order_no FROM raw.payment WHERE type=1 UNION SELECT txn_id,order_no FROM raw.original_payment
        ) GROUP BY order_no HAVING COUNT(*)>1''')
    for methods, headers in (('payment_method', 'payment'), ('original_payment_method', 'original_payment')):
        gates.check(f'{methods}:header', f'''SELECT m.pm_id FROM raw.{methods} m LEFT JOIN raw.{headers} p USING(txn_id)
            WHERE p.txn_id IS NULL OR p.order_no<>m.order_no OR p.type<>m.type OR m.amount<=0
            OR m.amount>1000000000000 OR m.method NOT IN (1,2,3,4) OR m.event_time<>p.event_time''')
        gates.check(f'{methods}:sum', f'''SELECT p.txn_id FROM raw.{headers} p LEFT JOIN raw.{methods} m USING(txn_id)
            GROUP BY p.txn_id,p.amount,p.type HAVING
            (p.type=1 AND COALESCE(SUM(m.amount),0)<>p.amount) OR
            (p.type=2 AND COUNT(m.pm_id)>0 AND SUM(m.amount)<>p.amount)''')
        gates.check(f'{methods}:one_mid', f'''SELECT txn_id,method FROM raw.{methods}
            GROUP BY txn_id,method HAVING COUNT(DISTINCT payments_mid)>1''')
        gates.check(f'{methods}:mid', f'''SELECT m.pm_id FROM raw.{methods} m JOIN raw.{headers} p USING(txn_id)
            LEFT JOIN raw.mid_map mm ON mm.payments_mid=m.payments_mid
            WHERE (m.method IN (1,2) AND (mm.payments_mid IS NULL OR mm.brand<>p.brand))
               OR (m.method IN (3,4) AND m.payments_mid IS NOT NULL)''')
    gates.check('fee_range', 'SELECT method FROM raw.method_fee WHERE fee_rate<0 OR fee_rate>1 OR method NOT IN (1,2,3,4)')
    gates.check('fee_available', '''SELECT m.method FROM (
        SELECT method FROM raw.payment_method WHERE type=1 UNION SELECT method FROM raw.original_payment_method
        ) m LEFT JOIN raw.method_fee f ON f.method=m.method AND f.effective_from<=?::DATE
        GROUP BY m.method HAVING COUNT(f.method)=0''', [day])
    actual = db.execute('''SELECT COUNT(*) FILTER(WHERE type=1),COUNT(*) FILTER(WHERE type=2),
        COALESCE(SUM(amount) FILTER(WHERE type=1),0),COALESCE(SUM(amount) FILTER(WHERE type=2),0) FROM raw.payment''').fetchone()
    expected = manifest['close']['expected']
    if tuple(expected[k] for k in ('pay_count','cancel_count','gross','cancel')) != actual:
        gates.check('upstream_control_totals', 'SELECT ?', [str(actual)])
    gates.check('upstream_control_totals', 'SELECT 1 WHERE false')


def output_checks(db, manifest, gates):
    gates.check('nonnegative_allocations', 'SELECT event_id FROM settlement_event WHERE allocated_amount<0 OR fee<0')
    gates.check('transaction_conservation', '''SELECT p.txn_id FROM raw.payment p LEFT JOIN settlement_event e USING(txn_id)
        GROUP BY p.txn_id,p.amount HAVING COUNT(e.event_id)=0 OR SUM(e.allocated_amount)<>p.amount''')
    gates.check('aggregate_conservation', '''SELECT 1 WHERE
        (SELECT COALESCE(SUM(gross),0) FROM settlement_agg) <>
        (SELECT COALESCE(SUM(amount),0) FROM raw.payment WHERE type=1) OR
        (SELECT COALESCE(SUM(cancel),0) FROM settlement_agg) <>
        (SELECT COALESCE(SUM(amount),0) FROM raw.payment WHERE type=2)''')
    gates.check('payout_formula', 'SELECT partner_id FROM settlement_agg WHERE net<>gross-cancel OR payout<>net-fee')
    gates.check('result_date', 'SELECT settlement_date FROM settlement_agg WHERE settlement_date<>?::DATE', [manifest['settlement_date']])


def compute(snapshot_path, manifest, work):
    work = Path(work)
    dbpath = work / 'work.duckdb'
    with duckdb.connect(str(dbpath)) as db:
        db.execute('SET threads=2')
        load_inputs(db, snapshot_path, manifest)
        gates = Gates(db, work / 'input-checks.json')
        input_checks(db, manifest, gates)
    env = os.environ.copy()
    env['FILE_BATCH_DB'] = str(dbpath.resolve())
    command = [str(Path(sys.executable).parent / 'dbt'), 'build', '--project-dir', str(ROOT / 'dbt'),
               '--profiles-dir', str(ROOT / 'dbt'), '--target-path', str(work / 'dbt-target'),
               '--log-path', str(work / 'dbt-logs')]
    with (work / 'dbt.log').open('w') as log:
        result = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT)
    if result.returncode:
        raise BatchError(f'dbt 실패: {work / "dbt.log"}')
    with duckdb.connect(str(dbpath)) as db:
        gates = Gates(db, work / 'output-checks.json')
        output_checks(db, manifest, gates)
        for name in ('settlement_event', 'settlement_agg'):
            out = str(work / (name + '.parquet')).replace("'", "''")
            db.execute(f"COPY (SELECT * FROM {name} ORDER BY ALL) TO '{out}' (FORMAT PARQUET)")
    return dbpath
