import os
from pathlib import Path
import re

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo
import pytest

from file_batch.store import Store


@pytest.fixture(scope='session')
def integration_dsn():
    dsn=os.environ.get('FILE_BATCH_TEST_DSN')
    if not dsn:
        pytest.skip('FILE_BATCH_TEST_DSN을 분리된 PostgreSQL에 설정하세요')
    info=conninfo_to_dict(dsn)
    if info.get('user')!='file_batch_test':
        pytest.fail('테스트 전용 file_batch_test 계정만 허용합니다')
    Store(dsn).init()
    return dsn


@pytest.fixture
def store(integration_dsn):
    store=Store(integration_dsn)
    with store.connect() as conn:
        conn.execute('TRUNCATE settlement_candidate,settlement_attempt,settlement_publication_audit,settlement_publication,settlement_run,settlement_agg RESTART IDENTITY')
    return store


@pytest.fixture(scope='session')
def source_dsns(integration_dsn):
    repo=Path(__file__).resolve().parents[2]
    init=(repo/'infra/postgres/init.sql').read_text()
    with psycopg.connect(integration_dsn,autocommit=True) as conn:
        for name in ('paydb','orderdb'):
            if not conn.execute('SELECT 1 FROM pg_database WHERE datname=%s',(name,)).fetchone():
                conn.execute('CREATE DATABASE '+name)
    dsns=[make_conninfo(integration_dsn,dbname=name) for name in ('paydb','orderdb')]
    pay=init.split('\\connect paydb')[1].split('\\connect postgres')[0]
    order=init.split('\\connect orderdb')[1]
    for dsn,script in zip(dsns,(pay,order)):
        script=re.sub(r'CREATE PUBLICATION.*?;', '',script,flags=re.S)
        with psycopg.connect(dsn) as conn:
            conn.execute('DROP SCHEMA public CASCADE; CREATE SCHEMA public')
            conn.execute(script)
    return dsns
