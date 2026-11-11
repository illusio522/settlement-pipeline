from file_batch import compute


def test_version_ignores_dbt_identity_but_tracks_dependency_lock(tmp_path, monkeypatch):
    monkeypatch.setattr(compute, 'ROOT', tmp_path)
    (tmp_path / 'model.sql').write_text('SELECT 1')
    lock = tmp_path / 'requirements.lock'
    lock.write_text('duckdb==1.5.3')
    before = compute.code_version()
    (tmp_path / '.user.yml').write_text('id: generated-on-first-dbt-run')
    assert compute.code_version() == before
    lock.write_text('duckdb==different-version')
    assert compute.code_version() != before
