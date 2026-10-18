"""Run the existing CDC landing loader + existing settlement SQL, unmodified business logic."""
from pathlib import Path
import re
import duckdb
from .bundle import BatchError

REPO=Path(__file__).resolve().parent.parent


def sql_script(path):
    return '\n'.join(line for line in Path(path).read_text().splitlines() if not line.lstrip().startswith('.'))


def legacy_from_landing(landing, output, masters=None):
    """Read a captured CDC landing. Never overwrite an existing reference DB."""
    landing=Path(landing).resolve()
    for table in ('payment','payment_method','order_item','cancel'):
        if not list(landing.glob(f'{table}.p*.ndjson')):
            raise BatchError(f'CDC 랜딩 파일 없음: {table}')
    output=Path(output).resolve()
    if output.exists():
        raise BatchError('기존 비교 DB를 덮어쓰지 않습니다')
    output.parent.mkdir(parents=True,exist_ok=True)
    with duckdb.connect(str(output)) as db:
        db.execute(sql_script(REPO/'transform/01_schema.sql'))
        query=sql_script(REPO/'transform/e2e_load.sql').replace('/work/data/landing/',str(landing).replace("'","''")+'/')
        db.execute(query)
        if masters is not None:
            # Real comparisons must use the same pinned master snapshot on both paths.
            for name,rows in masters.items():
                db.execute(f'DELETE FROM {name}')
                if rows:
                    cols=list(rows[0])
                    db.executemany(f'INSERT INTO {name} ({",".join(cols)}) VALUES ({",".join("?" for _ in cols)})',
                                   [tuple(r[c] for c in cols) for r in rows])
        db.execute(sql_script(REPO/'transform/03_settlement.sql'))
    return output
