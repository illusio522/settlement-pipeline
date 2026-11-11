"""파일 배치 단계별 성능 측정.

전용 벤치 PostgreSQL에 bench_seed.sql 구조의 합성 데이터를 적재한 뒤,
실제 file_batch 함수(export_bundle, run)를 그대로 호출하며 단계별 시간을 기록한다.
제품 코드는 수정하지 않고 모듈 함수를 계측용 래퍼로 감싼다.
DSN은 FILE_BATCH_PAY_DSN / FILE_BATCH_ORDER_DSN / FILE_BATCH_SERVING_DSN 환경변수로 주입한다.
"""
import argparse
from datetime import date, timedelta
import json
import os
from pathlib import Path
import resource
import sys
import tempfile
import time

import duckdb
import psycopg
from psycopg.conninfo import conninfo_to_dict

import file_batch.compute as compute_mod
import file_batch.runner as runner_mod
from file_batch.export import export_bundle
from file_batch.store import Store

REPO = Path(__file__).resolve().parents[1]
BENCH_USER = 'file_batch_bench'
# bench_seed.sql의 시각 분포 식. 원본이 바뀌면 치환이 조용히 빠지지 않도록 즉시 실패시킨다.
PAY_TIME = "TIMESTAMP '2026-05-15' + to_seconds((random()*2160000)::BIGINT)"
CANCEL_TIME = "TIMESTAMP '2026-06-02' + to_seconds(c.order_no % 600000)"
PAY_TABLES = ('payment', 'payment_card', 'payment_money', 'payment_point', 'payment_coupon')
ORDER_TABLES = ('order_item', 'cancel')
CONTROL_TABLES = ('settlement_candidate,settlement_attempt,settlement_publication_audit,'
                  'settlement_publication,settlement_run,settlement_agg')


class Recorder:
    """래핑한 함수의 누적 소요 시간과 정합성 검사별 시간을 모은다."""

    def __init__(self):
        self.stages, self.gates = {}, []

    def wrap(self, name, fn):
        def timed(*args, **kwargs):
            started = time.perf_counter()
            result = fn(*args, **kwargs)
            self.stages[name] = self.stages.get(name, 0.0) + time.perf_counter() - started
            return result
        return timed

    def wrap_gate(self, fn):
        def timed(gates, name, *args, **kwargs):
            started = time.perf_counter()
            result = fn(gates, name, *args, **kwargs)
            self.gates.append((name, time.perf_counter() - started))
            return result
        return timed


def instrument(rec):
    runner_mod.snapshot = rec.wrap('snapshot', runner_mod.snapshot)
    runner_mod.compute = rec.wrap('compute', runner_mod.compute)
    compute_mod.load_inputs = rec.wrap('load_inputs', compute_mod.load_inputs)
    compute_mod.input_checks = rec.wrap('input_checks', compute_mod.input_checks)
    compute_mod.output_checks = rec.wrap('output_checks', compute_mod.output_checks)
    compute_mod.Gates.check = rec.wrap_gate(compute_mod.Gates.check)
    for method in ('claim', 'stage', 'publish'):
        setattr(Store, method, rec.wrap(method, getattr(Store, method)))


def peak_rss_mb():
    # macOS는 바이트, Linux는 KB 단위로 보고한다.
    unit = 1 if sys.platform == 'darwin' else 1024
    own = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * unit
    child = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss * unit
    return round(own / 2**20), round(child / 2**20)


def seed_sql(day, days):
    seed = (REPO / 'bench/bench_seed.sql').read_text()
    for old in (PAY_TIME, CANCEL_TIME):
        if seed.count(old) != 1:
            raise SystemExit(f'bench_seed.sql time expression changed: {old}')
    start = date.fromisoformat(day) - timedelta(days=days - 1)
    # 결제는 대상일로 끝나는 days일에 균등 분산, 취소는 모두 대상일에 둔다.
    seed = seed.replace(PAY_TIME, f"TIMESTAMP '{start}' + to_seconds(floor(random()*{days * 86400})::BIGINT)")
    return seed.replace(CANCEL_TIME, f"TIMESTAMP '{day}' + to_seconds(c.order_no % 86400)")


def generate(db, orders, day, days):
    db.execute("SET memory_limit='8GB'; SET threads TO 4")
    db.execute((REPO / 'transform/01_schema.sql').read_text())
    db.execute(seed_sql(day, days).replace('__ORDERS__', str(orders)))


def require_bench_user(dsn):
    if conninfo_to_dict(dsn).get('user') != BENCH_USER:
        raise SystemExit(f'only the dedicated {BENCH_USER} account is allowed')


def copy_csv(conn, table, columns, path):
    with conn.cursor().copy(f'COPY {table} ({columns}) FROM STDIN WITH (FORMAT csv)') as copy:
        with open(path, 'rb') as f:
            while chunk := f.read(1 << 20):
                copy.write(chunk)


def load_source(db, pay_dsn, order_dsn, tmp):
    method_cols = 'pm_id,txn_id,order_no,{mid}amount,type,event_time'
    exports = {
        'payment': ('txn_id,order_no,brand,amount,type,orig_txn_id,event_time', 'SELECT * FROM payment'),
        'order_item': ('order_no,line_no,product_id,unit_price,item_amount,partner_id,brand',
                       'SELECT order_no,line_no,order_no,item_amount,item_amount,partner_id,brand FROM order_item'),
        'cancel': ('cancel_txn_id,order_no,line_no', 'SELECT * FROM cancel'),
    }
    for code, name in enumerate(('card', 'money', 'point', 'coupon'), 1):
        cols = method_cols.format(mid='payments_mid,' if code < 3 else '')
        exports['payment_' + name] = (cols, f'SELECT {cols} FROM payment_method WHERE method={code}')
    for table, (_, query) in exports.items():
        out = str(Path(tmp) / f'{table}.csv').replace("'", "''")
        db.execute(f"COPY ({query}) TO '{out}' (FORMAT csv, HEADER false)")
    for dsn, tables in ((pay_dsn, PAY_TABLES), (order_dsn, ORDER_TABLES)):
        with psycopg.connect(dsn) as conn:
            conn.execute('TRUNCATE ' + ','.join(tables))
            for table in tables:
                copy_csv(conn, table, exports[table][0], Path(tmp) / f'{table}.csv')
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute('ANALYZE')


def write_specs(db, day, root):
    def rows(query):
        cur = db.execute(query)
        names = [d[0] for d in cur.description]
        return [dict(zip(names, r)) for r in cur.fetchall()]
    masters = dict(version='bench-v1',
                   partner=rows('SELECT partner_id,name,brand FROM partner ORDER BY ALL'),
                   method_fee=rows('SELECT method,fee_rate::VARCHAR AS fee_rate,effective_from::VARCHAR AS effective_from '
                                   'FROM method_fee ORDER BY ALL'),
                   mid_map=rows('SELECT payments_mid,brand FROM mid_map ORDER BY ALL'))
    totals = db.execute('''SELECT COUNT(*) FILTER(WHERE type=1),COUNT(*) FILTER(WHERE type=2),
        COALESCE(SUM(amount) FILTER(WHERE type=1),0),COALESCE(SUM(amount) FILTER(WHERE type=2),0)
        FROM payment WHERE CAST(event_time AS DATE)=?::DATE''', [day]).fetchone()
    close = dict(ready=True, reference=f'bench-synthetic-{day}', settlement_date=day,
                 expected=dict(zip(('pay_count', 'cancel_count', 'gross', 'cancel'), map(int, totals))))
    (root / 'masters.json').write_text(json.dumps(masters))
    (root / 'close.json').write_text(json.dumps(close))
    return root / 'masters.json', root / 'close.json'


def reset_serving(dsn):
    Store(dsn).init()
    with psycopg.connect(dsn) as conn:
        conn.execute(f'TRUNCATE {CONTROL_TABLES} RESTART IDENTITY')


def dbt_timing(work):
    results = json.loads((work / 'dbt-target/run_results.json').read_text())
    models = sorted(((r['unique_id'].split('.')[-1], r['execution_time']) for r in results['results']),
                    key=lambda m: -m[1])
    return results['elapsed_time'], models


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--orders', type=int, required=True, help='전체 생성 주문 수')
    parser.add_argument('--days', type=int, default=2, help='결제를 분산할 일수(대상일 포함)')
    parser.add_argument('--date', default='2026-06-02')
    parser.add_argument('--out', required=True)
    args = parser.parse_args()
    if not 0 < args.orders < 100_000_000 or args.days < 1:
        # bench_seed의 취소 txn_id(100000000+order_no)가 결제 txn_id와 겹치지 않는 범위로 제한한다.
        raise SystemExit('orders must be in (0, 100000000) and days >= 1')
    pay_dsn, order_dsn, serve_dsn = (os.environ[k] for k in
                                     ('FILE_BATCH_PAY_DSN', 'FILE_BATCH_ORDER_DSN', 'FILE_BATCH_SERVING_DSN'))
    for dsn in (pay_dsn, order_dsn, serve_dsn):
        require_bench_user(dsn)
    out = Path(args.out).resolve()
    out.mkdir(parents=True)
    t = {}

    with tempfile.TemporaryDirectory(dir=out) as tmp:
        with duckdb.connect(str(Path(tmp) / 'seed.duckdb')) as db:
            started = time.perf_counter()
            generate(db, args.orders, args.date, args.days)
            t['seed_generate'] = time.perf_counter() - started
            source_rows = {name: db.execute(f'SELECT COUNT(*) FROM {name}').fetchone()[0]
                           for name in ('payment', 'payment_method', 'order_item', 'cancel')}
            masters, close = write_specs(db, args.date, out)
            started = time.perf_counter()
            load_source(db, pay_dsn, order_dsn, tmp)
            t['source_load'] = time.perf_counter() - started
    reset_serving(serve_dsn)

    rec = Recorder()
    instrument(rec)
    started = time.perf_counter()
    bundle = export_bundle(pay_dsn, order_dsn, args.date, out / 'input', masters, close)
    t['export'] = time.perf_counter() - started
    rss_export = peak_rss_mb()

    started = time.perf_counter()
    result = runner_mod.run(bundle, out / 'runs', serve_dsn, 0)
    t['run_total'] = time.perf_counter() - started
    rss_run = peak_rss_mb()
    work = Path(result['database']).parent
    dbt_elapsed, models = dbt_timing(work)
    stages = dict(rec.stages)

    started = time.perf_counter()
    rerun = runner_mod.run(bundle, out / 'runs', serve_dsn, 0)
    t['rerun_reused'] = time.perf_counter() - started
    if not rerun['reused'] or rerun['run_id'] != result['run_id']:
        raise SystemExit('rerun did not reuse the published run')

    manifest = json.loads((Path(bundle) / 'manifest.json').read_text())
    bundle_rows = {name: sum(e['rows'] for e in entries) for name, entries in manifest['tables'].items()}
    bundle_mb = sum(e['bytes'] for entries in manifest['tables'].values() for e in entries) / 2**20
    with duckdb.connect(str(result['database']), read_only=True) as db:
        events = db.execute('SELECT COUNT(*) FROM settlement_event').fetchone()[0]
        aggs = db.execute('SELECT COUNT(*) FROM settlement_agg').fetchone()[0]

    # compute 안에서 load/input/dbt/output을 뺀 나머지 = dbt 프로세스 기동·결과 Parquet 기록 등
    other = stages['compute'] - stages['load_inputs'] - stages['input_checks'] - stages['output_checks'] - dbt_elapsed
    breakdown = [
        ('export', t['export']), ('claim', stages['claim']), ('snapshot', stages['snapshot']),
        ('load_inputs', stages['load_inputs']), ('input_checks', stages['input_checks']),
        ('dbt_build', dbt_elapsed), ('compute_other', other), ('output_checks', stages['output_checks']),
        ('stage', stages['stage']), ('publish', stages['publish']),
    ]
    target_txn = bundle_rows['payment']
    report = dict(
        orders=args.orders, days=args.days, settlement_date=args.date, duckdb_threads='2 (file_batch fixed)',
        source_rows=source_rows, bundle_rows=bundle_rows, bundle_mb=round(bundle_mb, 1),
        settlement_event_rows=events, settlement_agg_rows=aggs, revision=result['revision'],
        seconds={k: round(v, 3) for k, v in {**t, **dict(breakdown)}.items()},
        txn_per_second={name: round(target_txn / sec) for name, sec in
                        (('export', t['export']), ('run_total', t['run_total']),
                         ('export_plus_run', t['export'] + t['run_total']))},
        peak_rss_mb=dict(after_export=dict(zip(('self', 'children'), rss_export)),
                         after_run=dict(zip(('self', 'children'), rss_run))),
        slowest_gates=[(n, round(s, 3)) for n, s in sorted(rec.gates, key=lambda g: -g[1])[:8]],
        dbt_models=[(n, round(s, 3)) for n, s in models],
        run_id=result['run_id'], work=str(work),
    )
    (out / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2))

    print(f"\n== orders={args.orders:,} days={args.days} date={args.date} ==")
    print(f"target day: payment {bundle_rows['payment']:,} (cancel {manifest['close']['expected']['cancel_count']:,}), "
          f"method {bundle_rows['payment_method']:,}, order_item {bundle_rows['order_item']:,}, "
          f"original_payment {bundle_rows['original_payment']:,}, bundle {bundle_mb:.1f} MB")
    print(f"result: settlement_event {events:,}, settlement_agg {aggs:,}, revision {result['revision']}")
    print(f"{'stage':<16}{'seconds':>10}{'share':>8}")
    total = t['export'] + t['run_total']
    for name, sec in breakdown:
        print(f'{name:<16}{sec:>10.2f}{sec / total:>8.0%}')
    print(f"{'export+run':<16}{total:>10.2f}   ({target_txn / total:,.0f} txn/s)")
    print(f"{'rerun(reused)':<16}{t['rerun_reused']:>10.2f}")
    print(f"setup (not counted): seed {t['seed_generate']:.1f}s, source load {t['source_load']:.1f}s")
    # 같은 프로세스에서 시드 생성·적재도 하므로 python 값은 배치 단독 사용량이 아니다.
    print(f"peak RSS MB: python(incl. seed/load) {rss_run[0]:,}, dbt child {rss_run[1]:,}")
    print('slowest gates: ' + ', '.join(f'{n} {s:.2f}s' for n, s in report['slowest_gates'][:5]))
    print(f'report: {out / "report.json"}')


if __name__ == '__main__':
    main()
