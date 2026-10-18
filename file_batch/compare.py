"""Exact, multiplicity-preserving comparison of independently computed results."""
from pathlib import Path
import duckdb
from .bundle import BatchError, atomic_json
from .compute import AGG_COLUMNS, EVENT_COLUMNS

EVENT_TYPES = ['VARCHAR','BIGINT','BIGINT','SMALLINT','INTEGER','SMALLINT','SMALLINT','SMALLINT','INTEGER',
               'BIGINT','DECIMAL(5,4)','BIGINT','BIGINT','TIMESTAMP','DATE']
AGG_TYPES = ['DATE','INTEGER','SMALLINT','SMALLINT'] + ['BIGINT']*6


def compare(legacy_db, file_db, day, report):
    for p in (legacy_db,file_db):
        if not Path(p).is_file():
            raise BatchError(f'비교 DB 없음: {p}')
    result = dict(settlement_date=day, matched=True, tables={})
    with duckdb.connect() as db:
        for alias,path in (('legacy',legacy_db),('fileversion',file_db)):
            escaped = str(Path(path).resolve()).replace("'", "''")
            db.execute(f"ATTACH '{escaped}' AS {alias} (READ_ONLY)")
        for name,cols,types in [('settlement_event',EVENT_COLUMNS,EVENT_TYPES),('settlement_agg',AGG_COLUMNS,AGG_TYPES)]:
            projection = ','.join(f'CAST({c} AS {t}) AS {c}' for c,t in zip(cols,types))
            date_literal = str(__import__('datetime').date.fromisoformat(day))
            for alias in ('legacy','fileversion'):
                db.execute(f"CREATE TEMP VIEW {alias}_{name} AS SELECT {projection} FROM {alias}.{name} "
                           f"WHERE settlement_date=DATE '{date_literal}'")
            a,b=f'legacy_{name}',f'fileversion_{name}'
            counts = [db.execute(f'SELECT COUNT(*) FROM {v}').fetchone()[0] for v in (a,b)]
            diffs=[]
            for left,right in ((a,b),(b,a)):
                query=f'SELECT * FROM {left} EXCEPT ALL SELECT * FROM {right}'
                n=db.execute('SELECT COUNT(*) FROM ('+query+')').fetchone()[0]
                sample=db.execute(query+' LIMIT 5').fetchall()
                diffs.append(dict(count=n,sample=[[str(x) for x in row] for row in sample]))
            ok=counts[0]==counts[1] and all(x['count']==0 for x in diffs)
            result['tables'][name]=dict(legacy_rows=counts[0],file_rows=counts[1],matched=ok,
                                       legacy_only=diffs[0],file_only=diffs[1])
            result['matched'] &= ok
    atomic_json(report,result)
    if not result['matched']:
        raise BatchError(f'CDC/파일 결과 불일치: {report}')
    return result
