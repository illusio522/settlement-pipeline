"""Source snapshots -> bounded Parquet batches. No CDC service dependency."""
from datetime import date, datetime, time, timedelta
import json
from pathlib import Path
import uuid

import psycopg
from psycopg.rows import dict_row

from .bundle import BatchError, BundleWriter, master_rows, utcnow

PAY_COLS = 'txn_id,order_no,brand,amount,type,orig_txn_id,event_time'
METHOD_SQL = ' UNION ALL '.join(
    f'SELECT pm_id,txn_id,order_no,{code}::smallint AS method,'
    f'{"payments_mid" if code < 3 else "NULL::integer AS payments_mid"},amount,type,event_time '
    f'FROM payment_{name}'
    for code, name in enumerate(('card', 'money', 'point', 'coupon'), 1)
)


def batches(conn, query, params=(), size=10000):
    with conn.cursor(name='export_' + uuid.uuid4().hex, row_factory=dict_row) as cur:
        cur.execute(query, params)
        while rows := cur.fetchmany(size):
            yield rows


def export_bundle(pay_dsn, order_dsn, day, root, masters, close_path):
    start = datetime.combine(date.fromisoformat(day), time())
    end = start + timedelta(days=1)
    close = json.loads(Path(close_path).read_text())
    master_version, master = master_rows(masters)
    writer = BundleWriter(root, day, close, master_version)
    scope = 'event_time >= %s AND event_time < %s'
    with psycopg.connect(pay_dsn) as pay, psycopg.connect(order_dsn) as order:
        for conn in (pay, order):
            conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
            conn.execute("SET LOCAL TIME ZONE 'Asia/Seoul'")
        writer.manifest['extraction']['pay_started_at'] = utcnow()
        invalid = pay.execute(f"SELECT count(*) FROM payment WHERE {scope} AND "
                              "(status NOT IN (1,2) OR currency <> 'KRW' OR type NOT IN (1,2))",
                              (start, end)).fetchone()[0]
        if invalid:
            raise BatchError('정의되지 않은 거래 상태/통화/유형')
        writer.write('payment', batches(pay, f'SELECT {PAY_COLS} FROM payment WHERE {scope}', (start, end)))
        writer.write('payment_method', batches(pay, f'SELECT m.* FROM ({METHOD_SQL}) m '
                     'JOIN payment p USING(txn_id) WHERE p.event_time >= %s AND p.event_time < %s', (start, end)))
        original_ids = f'SELECT orig_txn_id FROM payment WHERE {scope} AND type=2'
        writer.write('original_payment', batches(pay, f'SELECT {PAY_COLS} FROM payment '
                     f'WHERE txn_id IN ({original_ids})', (start, end)))
        writer.write('original_payment_method', batches(pay, f'SELECT m.* FROM ({METHOD_SQL}) m '
                     f'WHERE txn_id IN ({original_ids})', (start, end)))
        # Transfer only identifiers, in bounded chunks, between separate DB snapshots.
        writer.manifest['extraction']['order_started_at'] = utcnow()
        def order_batches():
            for ids in batches(pay, f'SELECT DISTINCT order_no FROM payment WHERE {scope}', (start, end)):
                yield from batches(order, 'SELECT order_no,line_no,partner_id,brand,item_amount '
                                   'FROM order_item WHERE order_no = ANY(%s)', ([r['order_no'] for r in ids],))
        def cancel_batches():
            for ids in batches(pay, f'SELECT txn_id FROM payment WHERE {scope} AND type=2', (start, end)):
                yield from batches(order, 'SELECT cancel_txn_id,order_no,line_no FROM cancel '
                                   'WHERE cancel_txn_id = ANY(%s)', ([r['txn_id'] for r in ids],))
        writer.write('order_item', order_batches())
        writer.write('cancel', cancel_batches())
        writer.manifest['extraction']['pay_finished_at'] = utcnow()
        writer.manifest['extraction']['order_finished_at'] = utcnow()
    for name, rows in master.items():
        writer.write(name, [rows])
    return writer.finish()
