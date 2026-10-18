import json

import pytest
from file_batch.bundle import BatchError
from file_batch.verify import verify
from .fixtures import bundle,data
from .test_bundle_compute import landing


def test_repeatable_two_path_verification_process(tmp_path):
    source=data()
    result=verify(bundle(tmp_path/'bundles',source),landing(tmp_path/'landing',source),tmp_path/'reports')
    assert result['matched']
    assert result['tables']['settlement_event']['legacy_rows']==6
    from pathlib import Path
    work=Path(result['work'])
    assert all(v['matched'] for v in json.loads((work/'input-comparison.json').read_text()).values())
    assert (work/'cdc-capture.json').is_file()


def test_mismatched_source_is_not_misreported_as_equivalent(tmp_path):
    source=data()
    b=bundle(tmp_path/'bundles',source)
    source['payment_method'][2]['pm_id']=99999  # Same calculated money; different input identity.
    with pytest.raises(BatchError,match='원천 입력'):
        verify(b,landing(tmp_path/'landing',source),tmp_path/'reports')
