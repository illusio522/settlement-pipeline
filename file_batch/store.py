"""PostgreSQL ownership fencing and date-scoped transactional publication."""
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
import uuid

import psycopg
from psycopg.rows import dict_row

from .bundle import BatchError
from .compute import AGG_COLUMNS


class Store:
    def __init__(self, dsn, lease_seconds=60):
        self.dsn, self.lease = dsn, lease_seconds

    @contextmanager
    def connect(self):
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            yield conn

    def init(self):
        with self.connect() as conn:
            conn.execute((Path(__file__).parent / 'sql/control.sql').read_text())

    def claim(self, key, manifest, digest, version, expected_revision):
        run_id, token = uuid.uuid4(), uuid.uuid4()
        with self.connect() as conn:
            conn.execute('''INSERT INTO settlement_run
                (run_id,run_key,settlement_date,bundle_id,manifest_hash,code_version,expected_revision,status)
                VALUES(%s,%s,%s,%s,%s,%s,%s,'READY') ON CONFLICT(run_key) DO NOTHING''',
                (run_id,key,manifest['settlement_date'],manifest['bundle_id'],digest,version,expected_revision))
            row = conn.execute('SELECT *, lease_until>now() AS live FROM settlement_run WHERE run_key=%s FOR UPDATE', (key,)).fetchone()
            if row['status'] == 'PUBLISHED':
                return row
            if row['expected_revision'] != expected_revision:
                raise BatchError('동일 실행의 expected_revision 변경 금지; 새로운 입력으로 재정산하세요')
            if row['status'] in ('RUNNING','VALIDATED') and row['live']:
                raise BatchError('같은 입력이 이미 실행 중입니다')
            if row['attempt']:
                conn.execute('''UPDATE settlement_attempt SET finished_at=COALESCE(finished_at,now()),
                    error=COALESCE(error,'lease expired') WHERE run_id=%s AND attempt=%s''', (row['run_id'],row['attempt']))
            updated = conn.execute('''UPDATE settlement_run SET status='RUNNING',owner_token=%s,
                lease_until=now()+%s*interval '1 second',attempt=attempt+1,error=NULL,updated_at=now()
                WHERE run_id=%s RETURNING *''', (token,self.lease,row['run_id'])).fetchone()
            conn.execute('INSERT INTO settlement_attempt(run_id,attempt,owner_token) VALUES(%s,%s,%s)',
                         (updated['run_id'],updated['attempt'],token))
            return updated

    def _owned(self, conn, run_id, token, statuses):
        row = conn.execute('SELECT *, lease_until>now() AS live FROM settlement_run WHERE run_id=%s FOR UPDATE', (run_id,)).fetchone()
        if not row or row['owner_token'] != token or not row['live'] or row['status'] not in statuses:
            raise BatchError('실행 소유권 또는 상태가 유효하지 않습니다')
        return row

    def heartbeat(self, run_id, token):
        with self.connect() as conn:
            row = conn.execute('''UPDATE settlement_run SET lease_until=now()+%s*interval '1 second',updated_at=now()
                WHERE run_id=%s AND owner_token=%s AND lease_until>now() AND status IN ('RUNNING','VALIDATED')
                RETURNING run_id''', (self.lease,run_id,token)).fetchone()
            if not row:
                raise BatchError('실행 소유권 만료')

    def fail(self, run_id, token, error):
        with self.connect() as conn:
            row = conn.execute('''UPDATE settlement_run SET status='FAILED',error=%s,updated_at=now()
                WHERE run_id=%s AND owner_token=%s AND status IN ('RUNNING','VALIDATED') RETURNING attempt''',
                (error,run_id,token)).fetchone()
            if row:
                conn.execute('UPDATE settlement_attempt SET finished_at=now(),error=%s WHERE run_id=%s AND attempt=%s',
                             (error,run_id,row['attempt']))

    def stage(self, run_id, token, rows):
        """Date aggregate is small; exact row comparison, not just count/sum."""
        with self.connect() as conn:
            run = self._owned(conn, run_id, token, ('RUNNING',))
            if any(row[0] != run['settlement_date'] for row in rows):
                raise BatchError('다른 정산일의 게시 후보')
            conn.execute('DELETE FROM settlement_candidate WHERE run_id=%s', (run_id,))
            cols = ','.join(['run_id','owner_token'] + AGG_COLUMNS)
            with conn.cursor().copy(f'COPY settlement_candidate ({cols}) FROM STDIN') as copy:
                for row in rows:
                    copy.write_row((run_id,token,*row))
            actual = conn.execute('SELECT '+','.join(AGG_COLUMNS)+' FROM settlement_candidate WHERE run_id=%s AND owner_token=%s',
                                  (run_id,token)).fetchall()
            if Counter(tuple(r[c] for c in AGG_COLUMNS) for r in actual) != Counter(rows):
                raise BatchError('DB 게시 후보가 검증 산출물과 다릅니다')
            conn.execute("UPDATE settlement_run SET status='VALIDATED',updated_at=now() WHERE run_id=%s", (run_id,))

    def publish(self, run_id, token, after_replace=None):
        with self.connect() as conn:
            run = self._owned(conn, run_id, token, ('VALIDATED',))
            day = run['settlement_date']
            conn.execute('INSERT INTO settlement_publication(settlement_date) VALUES(%s) ON CONFLICT DO NOTHING', (day,))
            pub = conn.execute('SELECT * FROM settlement_publication WHERE settlement_date=%s FOR UPDATE', (day,)).fetchone()
            if pub['finalized']:
                raise BatchError('확정된 마감일입니다. 사유를 기록해 재마감을 승인해야 합니다')
            if pub['revision'] != run['expected_revision']:
                raise BatchError('게시 버전 충돌: 오래된 실행은 최신 결과를 덮어쓸 수 없습니다')
            conn.execute('DELETE FROM settlement_agg WHERE settlement_date=%s', (day,))
            cols = ','.join(AGG_COLUMNS)
            conn.execute(f'INSERT INTO settlement_agg({cols}) SELECT {cols} FROM settlement_candidate WHERE run_id=%s AND owner_token=%s',
                         (run_id,token))
            if after_replace:  # Fault-injection seam for transaction rollback integration tests.
                after_replace()
            revision = pub['revision'] + 1
            conn.execute('''UPDATE settlement_publication SET revision=%s,run_id=%s,updated_at=now() WHERE settlement_date=%s''',
                         (revision,run_id,day))
            conn.execute("UPDATE settlement_run SET status='PUBLISHED',lease_until=NULL,updated_at=now() WHERE run_id=%s", (run_id,))
            conn.execute('UPDATE settlement_attempt SET finished_at=now() WHERE run_id=%s AND attempt=%s', (run_id,run['attempt']))
            conn.execute("INSERT INTO settlement_publication_audit(settlement_date,revision,action,reason) VALUES(%s,%s,'publish',%s)",
                         (day,revision,str(run_id)))
            return revision

    def finalization(self, day, revision, finalized, reason):
        if not reason.strip():
            raise BatchError('확정·재마감 사유가 필요합니다')
        with self.connect() as conn:
            pub = conn.execute('SELECT * FROM settlement_publication WHERE settlement_date=%s FOR UPDATE', (day,)).fetchone()
            if not pub or not pub['run_id'] or pub['revision'] != revision:
                raise BatchError('게시 버전 불일치 또는 게시 결과 없음')
            conn.execute('UPDATE settlement_publication SET finalized=%s,updated_at=now() WHERE settlement_date=%s', (finalized,day))
            conn.execute('INSERT INTO settlement_publication_audit(settlement_date,revision,action,reason) VALUES(%s,%s,%s,%s)',
                         (day,revision,'finalize' if finalized else 'reopen',reason))
