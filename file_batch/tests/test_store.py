from concurrent.futures import ThreadPoolExecutor
from datetime import date
import threading

import pytest

from file_batch.bundle import BatchError
from .fixtures import DAY


def claim(store,key='key',day=DAY,revision=0):
    return store.claim(key,dict(settlement_date=day,bundle_id='bundle'),key,'code',revision)


def rows(day=DAY,amount=100):
    return [(date.fromisoformat(day),1,1,1,amount,0,amount,2,amount-2,1)]


def finish(store,run,amount=100):
    store.stage(run['run_id'],run['owner_token'],rows(str(run['settlement_date']),amount))
    return store.publish(run['run_id'],run['owner_token'])


def test_same_run_concurrent_claim_and_completed_retry(store):
    barrier=threading.Barrier(2)
    def execute():
        barrier.wait()
        try: return claim(store)
        except BatchError: return None
    with ThreadPoolExecutor(2) as pool:
        results=list(pool.map(lambda _:execute(),range(2)))
    winners=[r for r in results if r]
    assert len(winners)==1
    assert finish(store,winners[0])==1
    repeated=claim(store)
    assert repeated['status']=='PUBLISHED'
    with store.connect() as conn:
        assert conn.execute('SELECT gross FROM settlement_agg').fetchone()['gross']==100
        assert conn.execute('SELECT COUNT(*) AS n FROM settlement_run').fetchone()['n']==1


def test_expired_worker_is_fenced(store):
    old=claim(store)
    with store.connect() as conn:
        conn.execute("UPDATE settlement_run SET lease_until=now()-interval '1 second'")
    new=claim(store)
    assert new['attempt']==2
    with pytest.raises(BatchError): store.heartbeat(old['run_id'],old['owner_token'])
    with pytest.raises(BatchError): store.stage(old['run_id'],old['owner_token'],rows())
    store.fail(old['run_id'],old['owner_token'],'stale failure')
    assert finish(store,new)==1


def test_failed_run_retries_same_input(store):
    old=claim(store)
    store.fail(old['run_id'],old['owner_token'],'simulated failure')
    new=claim(store)
    assert new['attempt']==2 and new['run_id']==old['run_id']
    assert finish(store,new)==1


def test_date_replacement_and_stale_publication(store):
    first=claim(store,'first')
    finish(store,first)
    other=claim(store,'other',day='2026-05-19')
    finish(store,other,200)
    stale=claim(store,'stale',revision=1)
    fresh=claim(store,'fresh',revision=1)
    finish(store,fresh,300)
    store.stage(stale['run_id'],stale['owner_token'],rows(amount=50))
    with pytest.raises(BatchError,match='버전 충돌'):
        store.publish(stale['run_id'],stale['owner_token'])
    with store.connect() as conn:
        actual=conn.execute('SELECT settlement_date,gross FROM settlement_agg ORDER BY settlement_date').fetchall()
    assert [r['gross'] for r in actual]==[200,300]


def test_publication_rollback_and_response_loss(store):
    finish(store,claim(store,'initial'))
    update=claim(store,'update',revision=1)
    store.stage(update['run_id'],update['owner_token'],rows(amount=200))
    def crash(): raise RuntimeError('before commit')
    with pytest.raises(RuntimeError): store.publish(update['run_id'],update['owner_token'],after_replace=crash)
    with store.connect() as conn:
        assert conn.execute('SELECT gross FROM settlement_agg').fetchone()['gross']==100
        assert conn.execute('SELECT revision FROM settlement_publication').fetchone()['revision']==1
    assert store.publish(update['run_id'],update['owner_token'])==2
    # Simulate successful commit followed by lost client response.
    assert claim(store,'update',revision=1)['status']=='PUBLISHED'


def test_finalization_requires_explicit_reopen(store):
    finish(store,claim(store,'first'))
    store.finalization(DAY,1,True,'daily close')
    update=claim(store,'update',revision=1)
    store.stage(update['run_id'],update['owner_token'],rows(amount=200))
    with pytest.raises(BatchError,match='확정'): store.publish(update['run_id'],update['owner_token'])
    with pytest.raises(BatchError): store.finalization(DAY,1,False,'')
    store.finalization(DAY,1,False,'approved correction')
    assert store.publish(update['run_id'],update['owner_token'])==2


def test_empty_day_replacement_and_wrong_day_rejection(store):
    finish(store,claim(store,'initial'))
    update=claim(store,'empty',revision=1)
    with pytest.raises(BatchError): store.stage(update['run_id'],update['owner_token'],rows('2026-05-19'))
    store.stage(update['run_id'],update['owner_token'],[])
    store.publish(update['run_id'],update['owner_token'])
    with store.connect() as conn:
        assert conn.execute('SELECT COUNT(*) AS n FROM settlement_agg').fetchone()['n']==0


def test_invalid_candidate_rolls_back(store):
    run=claim(store)
    invalid=list(rows()[0]); invalid[8]+=1
    import psycopg
    with pytest.raises(psycopg.errors.CheckViolation):
        store.stage(run['run_id'],run['owner_token'],[tuple(invalid)])
    with store.connect() as conn:
        assert conn.execute('SELECT COUNT(*) AS n FROM settlement_candidate').fetchone()['n']==0
        assert conn.execute('SELECT status FROM settlement_run').fetchone()['status']=='RUNNING'
