"""Real transport acceptance test. Dedicated compose stack only; never existing user services."""
import json
from pathlib import Path
import re
import subprocess
import time
from urllib.request import Request, urlopen
from urllib.error import URLError

import psycopg

from file_batch.bundle import atomic_json, BatchError
from file_batch.export import export_bundle
from file_batch.runner import run
from file_batch.store import Store
from file_batch.verify import verify
from .fixtures import DAY, data, close_for, master_json
from .test_export_run import seed

REPO=Path(__file__).resolve().parents[2]
DSN='postgresql://file_batch_test:file_batch_test_local@127.0.0.1:55440/'
CONNECT='http://127.0.0.1:55443'


def request(path,payload=None):
    req=Request(CONNECT+path,data=None if payload is None else json.dumps(payload).encode(),
                headers={'Content-Type':'application/json'})
    with urlopen(req,timeout=10) as response:
        return json.loads(response.read())


def wait_landed(process,expected):
    deadline=time.monotonic()+120
    while time.monotonic()<deadline:
        if process.poll() is not None:
            raise BatchError('Java 컨슈머가 조기 종료했습니다. consumer.log 확인')
        try:
            with urlopen('http://127.0.0.1:55444/actuator/prometheus',timeout=3) as response:
                metrics=response.read().decode()
            match=re.search(r'^settlement_landed_records_total(?:\{[^\n]*\})?\s+([0-9.eE+]+)',metrics,re.M)
            if match and float(match[1])>=expected:
                return
        except (URLError,TimeoutError):
            pass
        time.sleep(1)
    raise BatchError(f'CDC 소비 시간 초과: {expected}건')


def main():
    import uuid
    work=REPO/'data/file-batch/live-verification'/str(uuid.uuid4())
    work.mkdir(parents=True)
    # Require a fresh dedicated test stack to avoid offsets/snapshot state contaminating the test.
    if request('/connectors'):
        raise BatchError('새 CDC 검증 스택이 필요합니다. 전용 스택을 down -v 후 다시 기동하세요')
    source=data()
    seed((DSN+'paydb',DSN+'orderdb'),source)
    jar=REPO/'normalize/build/libs/settlement-normalize-0.1.0.jar'
    if not jar.is_file():
        raise BatchError('normalize bootJar 빌드가 필요합니다')
    landing=work/'landing'
    args=['java','-jar',str(jar),'--server.port=55444','--spring.kafka.bootstrap-servers=127.0.0.1:55441',
          '--spring.kafka.consumer.group-id=file-batch-live-verify',
          '--app.registry-url=http://127.0.0.1:55442/apis/registry/v2',f'--app.landing-dir={landing}']
    with (work/'consumer.log').open('w') as log:
        consumer=subprocess.Popen(args,cwd=REPO/'normalize',stdout=log,stderr=subprocess.STDOUT)
        try:
            expected=sum(len(source[t]) for t in ('payment','payment_method','order_item','cancel'))
            # Registry 2.4.1 uses a shared date parser; serial initial registration avoids its concurrent parse failure.
            seen=0
            for domain in ('pay','order'):
                spec=json.loads((REPO/f'infra/debezium/{domain}-connector.json').read_text())
                spec['config']['database.user']='file_batch_test'
                spec['config']['database.password']='file_batch_test_local'
                request('/connectors',spec)
                seen+=sum(len(source[t]) for t in (('payment','payment_method') if domain=='pay' else ('order_item','cancel')))
                wait_landed(consumer,seen)
            # Real WAL UPDATE and DELETE in addition to the initial snapshot.
            with psycopg.connect(DSN+'paydb') as conn:
                conn.execute('UPDATE payment SET amount=102 WHERE txn_id=200')
                conn.execute('UPDATE payment_card SET amount=87 WHERE pm_id=2001')
                conn.execute("INSERT INTO payment(txn_id,order_no,brand,amount,type,event_time) VALUES(999,999,1,1,1,'2026-05-20')")
                conn.execute('DELETE FROM payment WHERE txn_id=999')
            with psycopg.connect(DSN+'orderdb') as conn:
                conn.execute('UPDATE order_item SET item_amount=52,unit_price=52 WHERE order_no=20 AND line_no=2')
            expected+=5
            wait_landed(consumer,expected)
        finally:
            consumer.terminate()
            try: consumer.wait(timeout=30)
            except subprocess.TimeoutExpired:
                consumer.kill();consumer.wait()
                raise BatchError('정상 flush 종료 실패')
    # Source fixture is updated only to form independent upstream control totals/master input.
    source['payment'][1]['amount']=102
    source['payment_method'][2]['amount']=87
    source['order_item'][-1]['item_amount']=52
    master=work/'masters.json';master_json(master,source)
    close=work/'close.json';atomic_json(close,close_for(source))
    bundle=export_bundle(DSN+'paydb',DSN+'orderdb',DAY,work/'bundles',master,close)
    store=Store(DSN+'servedb');store.init()
    published=run(bundle,work/'runs',store.dsn)
    repeated=run(bundle,work/'runs',store.dsn)
    assert repeated['reused'] and repeated['run_id']==published['run_id']
    comparison=verify(bundle,landing,work/'comparison')
    result=dict(matched=True,source='real PostgreSQL WAL -> Debezium Avro -> Kafka -> Java consumer',
                operations=['snapshot','update','insert','delete'],expected_landed=expected,
                work=str(work),publication=published,comparison=comparison)
    atomic_json(work/'acceptance.json',result)
    print(json.dumps(result,ensure_ascii=False))


if __name__=='__main__':
    main()
