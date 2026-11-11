"""CDC 경로 단계별 성능 측정 (파일 배치 측정과 같은 데이터·규모·DuckDB 스레드).

PostgreSQL -> Debezium(Avro) -> Kafka -> 기존 Java 컨슈머 -> NDJSON 랜딩
-> 기존 e2e_load.sql -> 기존 03_settlement.sql -> servedb 게시 순으로 실행하며 단계별 시간을 기록한다.
업무 로직 SQL은 수정하지 않는다. 03_settlement.sql의 `RESET threads;`만 실행 시 `SET threads=2;`로 바꾼다.
DSN은 CDC_BENCH_PG_DSN(데이터베이스 이름 제외) 환경변수로 주입한다.
"""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time
from urllib.error import URLError
from urllib.request import Request, urlopen

import duckdb
import psycopg
from psycopg.conninfo import conninfo_to_dict

from file_batch.reference import sql_script
from file_batch.store import Store
from file_batch_bench import REPO, generate, load_source, write_specs

BENCH_USER = 'file_batch_test'  # compose.cdc-test.yml이 고정한 전용 테스트 계정
CONNECT = 'http://127.0.0.1:55443'
METRICS = 'http://127.0.0.1:55444/actuator/prometheus'
JAR = REPO / 'normalize/build/libs/settlement-normalize-0.1.0.jar'
AGG_COLUMNS = 'settlement_date,partner_id,brand,method,gross,cancel,net,fee,payout,txn_count'
DUCKDB_THREADS = 2  # 파일 배치(file_batch/compute.py)와 같은 값


def request(path, payload=None):
    req = Request(CONNECT + path, data=None if payload is None else json.dumps(payload).encode(),
                  headers={'Content-Type': 'application/json'})
    with urlopen(req, timeout=10) as response:
        return json.loads(response.read())


def metric(text, name):
    match = re.search(rf'^{name}(?:\{{[^\n]*\}})?\s+([0-9.eE+]+)', text, re.M)
    return int(float(match[1])) if match else 0


def landed():
    try:
        with urlopen(METRICS, timeout=3) as response:
            text = response.read().decode()
    except (URLError, TimeoutError, ConnectionError):
        return None
    return metric(text, 'settlement_landed_records_total'), metric(text, 'settlement_dlq_records_total')


def wait_landed(consumer, expected, stall_seconds):
    """랜딩 건수가 expected에 도달한 시각을 반환한다. stall_seconds 동안 증가가 없으면 실패한다."""
    last, last_change = -1, time.perf_counter()
    while True:
        if consumer.poll() is not None:
            raise SystemExit('consumer exited early; see consumer.log')
        state = landed()
        now = time.perf_counter()
        if state:
            count, dlq = state
            if dlq:
                raise SystemExit(f'consumer sent {dlq} records to DLQ')
            if count > last:
                last, last_change = count, now
            if count >= expected:
                return now
        if now - last_change > stall_seconds:
            raise SystemExit(f'landing stalled at {last} of {expected} records')
        time.sleep(0.5)


def container_mb(container, path):
    out = subprocess.run(['docker', 'exec', container, 'du', '-sm', path], capture_output=True, text=True, check=True)
    return int(out.stdout.split()[0])


def dir_mb(path):
    return round(sum(p.stat().st_size for p in Path(path).rglob('*') if p.is_file()) / 2**20, 1)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--orders', type=int, required=True, help='전체 생성 주문 수')
    parser.add_argument('--days', type=int, default=2, help='결제를 분산할 일수(대상일 포함)')
    parser.add_argument('--date', default='2026-06-02')
    parser.add_argument('--out', required=True)
    parser.add_argument('--project', required=True, help='Docker Compose 프로젝트 이름(볼륨 크기 측정용)')
    parser.add_argument('--stall-seconds', type=int, default=300)
    args = parser.parse_args()
    if not 0 < args.orders < 100_000_000 or args.days < 1:
        raise SystemExit('orders must be in (0, 100000000) and days >= 1')
    base = os.environ['CDC_BENCH_PG_DSN']
    info = conninfo_to_dict(base)
    if info.get('user') != BENCH_USER:
        raise SystemExit(f'only the dedicated {BENCH_USER} account is allowed')
    pay_dsn, order_dsn, serve_dsn = (base.rstrip('/') + '/' + db for db in ('paydb', 'orderdb', 'servedb'))
    if request('/connectors'):
        raise SystemExit('connectors already registered; start a fresh CDC bench stack')
    if not JAR.is_file():
        raise SystemExit(f'consumer jar missing: {JAR}')
    out = Path(args.out).resolve()
    out.mkdir(parents=True)
    landing = out / 'landing'
    t = {}

    with tempfile.TemporaryDirectory(dir=out) as tmp:
        with duckdb.connect(str(Path(tmp) / 'seed.duckdb')) as db:
            started = time.perf_counter()
            generate(db, args.orders, args.date, args.days)
            t['seed_generate'] = time.perf_counter() - started
            source_rows = {name: db.execute(f'SELECT COUNT(*) FROM {name}').fetchone()[0]
                           for name in ('payment', 'payment_method', 'order_item', 'cancel')}
            _, close_path = write_specs(db, args.date, out)
            started = time.perf_counter()
            load_source(db, pay_dsn, order_dsn, tmp)
            t['source_load'] = time.perf_counter() - started
    close = json.loads(close_path.read_text())['expected']
    pay_rows = source_rows['payment'] + source_rows['payment_method']
    total_rows = sum(source_rows.values())

    command = ['java', '-jar', str(JAR), '--server.port=55444', '--spring.kafka.bootstrap-servers=127.0.0.1:55441',
               '--spring.kafka.consumer.group-id=cdc-bench', '--logging.level.com.settlement=WARN',
               '--app.registry-url=http://127.0.0.1:55442/apis/registry/v2', f'--app.landing-dir={landing}']
    with (out / 'consumer.log').open('w') as log:
        consumer = subprocess.Popen(command, cwd=REPO / 'normalize', stdout=log, stderr=subprocess.STDOUT)
        try:
            while landed() is None:
                if consumer.poll() is not None:
                    raise SystemExit('consumer exited during startup; see consumer.log')
                time.sleep(0.5)
            # Apicurio 2.4.1 동시 스키마 등록 오류를 피하려고 기존 검증과 같이 결제 -> 주문 순서로 등록한다.
            marks = {}
            for domain, expected in (('pay', pay_rows), ('order', total_rows)):
                spec = json.loads((REPO / f'infra/debezium/{domain}-connector.json').read_text())
                spec['config']['database.user'] = info['user']
                spec['config']['database.password'] = info['password']
                marks[f'{domain}_registered'] = time.perf_counter()
                request('/connectors', spec)
                marks[f'{domain}_landed'] = wait_landed(consumer, expected, args.stall_seconds)
            t['transport_pay'] = marks['pay_landed'] - marks['pay_registered']
            t['transport_order'] = marks['order_landed'] - marks['order_registered']
            t['transport'] = marks['order_landed'] - marks['pay_registered']
        finally:
            # 정상 종료(SIGTERM)해야 LandingWriter가 남은 버퍼를 flush한다.
            started = time.perf_counter()
            consumer.terminate()
            try:
                consumer.wait(timeout=120)
            except subprocess.TimeoutExpired:
                consumer.kill()
                consumer.wait()
                raise SystemExit('consumer did not stop within 120s; landing may be incomplete')
            t['consumer_stop'] = time.perf_counter() - started
    if consumer.returncode not in (0, 143):
        raise SystemExit(f'consumer did not stop cleanly: exit {consumer.returncode}')
    kafka_mb = container_mb(f'{args.project}-kafka-1', '/tmp/kafka-logs')
    pg_mb = container_mb(f'{args.project}-postgres-1', '/var/lib/postgresql/data')
    landing_mb = dir_mb(landing)

    dbpath = out / 'cdc.duckdb'
    with duckdb.connect(str(dbpath)) as db:
        db.execute(f'SET threads={DUCKDB_THREADS}')
        db.execute(sql_script(REPO / 'transform/01_schema.sql'))
        load_sql = sql_script(REPO / 'transform/e2e_load.sql').replace(
            '/work/data/landing/', str(landing).replace("'", "''") + '/')
        started = time.perf_counter()
        db.execute(load_sql)
        t['landing_load'] = time.perf_counter() - started
        loaded = {name: db.execute(f'SELECT COUNT(*) FROM {name}').fetchone()[0] for name in source_rows}
        if loaded != source_rows:
            raise SystemExit(f'landing rows differ from source: {loaded} != {source_rows}')
        settle = sql_script(REPO / 'transform/03_settlement.sql')
        if settle.count('RESET threads;') != 1:
            raise SystemExit('03_settlement.sql threads statement changed')
        settle = settle.replace('RESET threads;', f'SET threads={DUCKDB_THREADS};')
        started = time.perf_counter()
        db.execute(settle)
        t['settlement'] = time.perf_counter() - started
        events = db.execute('SELECT COUNT(*) FROM settlement_event').fetchone()[0]
        day_events = db.execute('SELECT COUNT(*) FROM settlement_event WHERE settlement_date=?::DATE',
                                [args.date]).fetchone()[0]
        recon_failed = db.execute('SELECT (SELECT COUNT(*) FROM recon_result WHERE status<>0)'
                                  '+(SELECT COUNT(*) FROM recon_card WHERE status<>0)').fetchone()[0]
        day_totals = db.execute('SELECT SUM(gross),SUM(cancel) FROM settlement_agg WHERE settlement_date=?::DATE',
                                [args.date]).fetchone()
        if recon_failed or tuple(map(int, day_totals)) != (close['gross'], close['cancel']):
            raise SystemExit(f'settlement check failed: recon={recon_failed} day_totals={day_totals}')
        started = time.perf_counter()
        agg = db.execute(f'SELECT {AGG_COLUMNS} FROM settlement_agg ORDER BY ALL').fetchall()
        dates = db.execute('SELECT COUNT(DISTINCT settlement_date) FROM settlement_agg').fetchone()[0]

    # 기존 publish-serving.sh와 같은 방식: 한 트랜잭션에서 TRUNCATE 후 COPY.
    Store(serve_dsn).init()
    with psycopg.connect(serve_dsn) as conn:
        conn.execute('TRUNCATE settlement_agg')
        with conn.cursor().copy(f'COPY settlement_agg ({AGG_COLUMNS}) FROM STDIN') as copy:
            for row in agg:
                copy.write_row(row)
    t['publish'] = time.perf_counter() - started

    after_close = t['landing_load'] + t['settlement'] + t['publish']
    total = t['transport'] + t['consumer_stop'] + after_close
    target_txn = close['pay_count'] + close['cancel_count']
    report = dict(
        path='cdc', orders=args.orders, days=args.days, settlement_date=args.date,
        duckdb_threads=DUCKDB_THREADS, source_rows=source_rows, cdc_records=total_rows,
        target_day_txn=target_txn, settlement_event_rows=events, target_day_event_rows=day_events,
        settlement_agg_rows=len(agg), settled_dates=dates,
        storage_mb=dict(kafka=kafka_mb, postgres=pg_mb, landing=landing_mb),
        seconds={k: round(v, 3) for k, v in {**t, 'after_close': after_close, 'total': total}.items()},
        records_per_second=dict(transport=round(total_rows / t['transport'])),
        txn_per_second=dict(total=round(target_txn / total)),
    )
    (out / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2))

    print(f"\n== CDC orders={args.orders:,} days={args.days} date={args.date} ==")
    print(f"CDC records {total_rows:,} (target day txn {target_txn:,}), "
          f"settlement_event {events:,} (target day {day_events:,}), settled dates {dates}")
    print(f"{'stage':<16}{'seconds':>10}{'share':>8}")
    for name in ('transport_pay', 'transport_order', 'consumer_stop', 'landing_load', 'settlement', 'publish'):
        print(f'{name:<16}{t[name]:>10.2f}{t[name] / total:>8.0%}')
    print(f"{'total':<16}{total:>10.2f}   ({target_txn / total:,.0f} txn/s, transport {total_rows / t['transport']:,.0f} rec/s)")
    print(f"{'after_close':<16}{after_close:>10.2f}   (landing load + settlement + publish)")
    print(f"setup (not counted): seed {t['seed_generate']:.1f}s, source load {t['source_load']:.1f}s")
    print(f"storage MB: kafka {kafka_mb:,}, postgres {pg_mb:,}, landing {landing_mb:,}")
    print(f'report: {out / "report.json"}')


if __name__ == '__main__':
    main()
