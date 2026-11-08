import pyarrow as pa
import pyarrow.parquet as pq

from scripts.restore_previous_snapshot_baseline import restore_baselines


def test_restores_full_source_row_count_baselines(tmp_path):
    active = tmp_path / "active.parquet"
    cleared = tmp_path / "cleared.parquet"
    pq.write_table(pa.Table.from_pylist([{"a": 1}, {"a": 2}]), active)
    pq.write_table(pa.Table.from_pylist([{"b": 1}, {"b": 2}, {"b": 3}]), cleared)

    counts = restore_baselines(active, cleared, tmp_path / "manifests")

    assert counts == {"active_permits": 2, "cleared_permits": 3}
    assert (
        tmp_path / "manifests/active_permits/00000000000000_published_baseline.json"
    ).is_file()
    assert (
        tmp_path / "manifests/cleared_permits/00000000000000_published_baseline.json"
    ).is_file()
