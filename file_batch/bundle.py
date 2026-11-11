"""Typed immutable input bundles; only manifest-listed files are consumed."""
from __future__ import annotations

import hashlib
import json
import shutil
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


class BatchError(RuntimeError):
    pass


def schema(fields):
    return pa.schema([pa.field(name, dtype, nullable=nullable) for name, dtype, nullable in fields])


I, S, T = pa.int64(), pa.int16(), pa.timestamp('us')
PAY = schema([('txn_id', I, False), ('order_no', I, False), ('brand', S, False),
              ('amount', I, False), ('type', S, False), ('orig_txn_id', I, True),
              ('event_time', T, False)])
METHOD = schema([('pm_id', I, False), ('txn_id', I, False), ('order_no', I, False),
                 ('method', S, False), ('payments_mid', pa.int32(), True),
                 ('amount', I, False), ('type', S, False), ('event_time', T, False)])
SCHEMAS = {
    'payment': PAY, 'payment_method': METHOD,
    'original_payment': PAY, 'original_payment_method': METHOD,
    'order_item': schema([('order_no', I, False), ('line_no', S, False),
                          ('partner_id', pa.int32(), False), ('brand', S, False),
                          ('item_amount', I, False)]),
    'cancel': schema([('cancel_txn_id', I, False), ('order_no', I, False), ('line_no', S, False)]),
    'partner': schema([('partner_id', pa.int32(), False), ('name', pa.string(), False), ('brand', S, False)]),
    'method_fee': schema([('method', S, False), ('fee_rate', pa.decimal128(5, 4), False),
                          ('effective_from', pa.date32(), False)]),
    'mid_map': schema([('payments_mid', pa.int32(), False), ('brand', S, False)]),
}


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def json_bytes(obj):
    return json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()


def sha256(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def atomic_json(path, obj):
    path = Path(path)
    tmp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    with tmp.open('xb') as f:
        f.write(json_bytes(obj))
        f.flush()
        import os
        os.fsync(f.fileno())
    tmp.replace(path)


def master_rows(path):
    obj = json.loads(Path(path).read_text())
    if not obj.get('version'):
        raise BatchError('기준정보 version이 필요합니다')
    rows = {t: obj[t] for t in ('partner', 'method_fee', 'mid_map')}
    for row in rows['method_fee']:
        row['fee_rate'] = Decimal(str(row['fee_rate']))
        row['effective_from'] = date.fromisoformat(row['effective_from'])
    return obj['version'], rows


def validate_close(close, day):
    if close.get('ready') is not True or close.get('settlement_date') != day or not close.get('reference'):
        raise BatchError('정산일과 일치하는 상류 마감 완료 명세가 필요합니다')
    expected = close.get('expected', {})
    for key in ('pay_count', 'cancel_count', 'gross', 'cancel'):
        if type(expected.get(key)) is not int or expected[key] < 0:
            raise BatchError(f'상류 컨트롤 토탈 누락/오류: {key}')


class BundleWriter:
    def __init__(self, root, day, close, master_version):
        date.fromisoformat(day)
        validate_close(close, day)
        self.bundle_id = str(uuid.uuid4())
        parent = Path(root) / f'date={day}'
        parent.mkdir(parents=True, exist_ok=True)
        self.final = parent / f'bundle={self.bundle_id}'
        self.tmp = parent / f'.{self.bundle_id}.partial'
        self.tmp.mkdir()
        self.manifest = dict(schema_version=1, bundle_id=self.bundle_id, settlement_date=day,
                             timezone='Asia/Seoul', master_version=master_version,
                             close=close, extraction={}, tables={})

    def write(self, name, batches):
        spec = SCHEMAS[name]
        folder = self.tmp / name
        folder.mkdir()
        entries = []
        for n, rows in enumerate(batches):
            if not rows:
                continue
            table = pa.Table.from_pylist(rows, schema=spec)
            entries.append(self._part(folder, n, table))
        if not entries:
            entries.append(self._part(folder, 0, pa.Table.from_pylist([], schema=spec)))
        self.manifest['tables'][name] = entries

    def _part(self, folder, n, table):
        path = folder / f'part-{n:06d}.parquet'
        pq.write_table(table, path, compression='zstd')
        return dict(path=path.relative_to(self.tmp).as_posix(), rows=table.num_rows,
                    bytes=path.stat().st_size, sha256=sha256(path))

    def finish(self):
        if set(self.manifest['tables']) != set(SCHEMAS):
            raise BatchError('필수 입력 테이블 누락')
        atomic_json(self.tmp / 'manifest.json', self.manifest)
        self.tmp.rename(self.final)
        return self.final


def read_manifest(bundle):
    path = Path(bundle) / 'manifest.json'
    if path.is_symlink() or not path.is_file():
        raise BatchError('완료된 manifest가 없습니다')
    raw = path.read_bytes()
    m = json.loads(raw)
    if m.get('schema_version') != 1 or m.get('timezone') != 'Asia/Seoul':
        raise BatchError('지원하지 않는 스키마/시간대')
    date.fromisoformat(m['settlement_date'])
    validate_close(m['close'], m['settlement_date'])
    if set(m.get('tables', {})) != set(SCHEMAS) or not m.get('master_version'):
        raise BatchError('manifest 테이블/기준정보 버전 오류')
    return m, hashlib.sha256(json_bytes(m)).hexdigest()


def snapshot(bundle, target, manifest):
    """Copy then verify. Computation never rereads mutable external input paths."""
    base, target = Path(bundle).resolve(), Path(target)
    target.mkdir(parents=True, exist_ok=False)
    used = set()
    for name, entries in manifest['tables'].items():
        if not entries:
            raise BatchError(f'파일 목록 없음: {name}')
        for entry in entries:
            relative = Path(entry['path'])
            src = (base / relative).resolve()
            if relative.is_absolute() or '..' in relative.parts or not src.is_relative_to(base):
                raise BatchError('bundle 외부 경로 금지')
            if relative.as_posix() in used:
                raise BatchError('중복 파일 경로')
            used.add(relative.as_posix())
            dst = target / relative
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src, dst)
            if dst.stat().st_size != entry['bytes'] or sha256(dst) != entry['sha256']:
                raise BatchError(f'파일 무결성 실패: {name}')
            pf = pq.ParquetFile(dst)
            if pf.metadata.num_rows != entry['rows'] or not pf.schema_arrow.equals(SCHEMAS[name]):
                raise BatchError(f'파일 건수/스키마 오류: {name}')
    atomic_json(target / 'manifest.json', manifest)
    return target
