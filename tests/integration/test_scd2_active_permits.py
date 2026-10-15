from dataclasses import replace
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from toronto_housing_ingestion.config import load_config
from toronto_housing_ingestion.dlt_loader import load_scd2

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_DIR = PROJECT_ROOT / "data" / "raw" / "active_permits"

snapshots = list(SOURCE_DIR.glob("*/*/data.parquet"))

if not snapshots:
    raise FileNotFoundError(f"No Active Permits snapshot found in {SOURCE_DIR}")

SOURCE_FILE = max(
    snapshots,
    key=lambda path: path.parent.name,
)


def test_active_permits_scd2(tmp_path):
    print("\n[1/6] Reading 10 records from the 2020+ snapshot...")

    source_table = pq.read_table(SOURCE_FILE)
    source_df = source_table.to_pandas()

    source_df = (
        source_df[
            pd.to_datetime(
                source_df["APPLICATION_DATE"],
                errors="coerce",
            )
            >= pd.Timestamp("2020-01-01")
        ]
        .head(10)
        .copy()
    )

    assert len(source_df) == 10, (
        f"Test setup failed: expected 10 rows, got {len(source_df)}"
    )

    print(f"    rows: {len(source_df)}")

    print("[2/6] Creating changed snapshot...")

    updated = source_df.copy()

    # Change one existing permit.
    changed_id = int(updated.iloc[0]["_id"])
    original_permit_type = updated.iloc[0]["PERMIT_TYPE"]

    updated.loc[updated.index[0], "PERMIT_TYPE"] = f"{original_permit_type}_SCD2_TEST"

    assert updated.iloc[0]["PERMIT_TYPE"] != original_permit_type, (
        "Test setup failed: permit_type did not actually change"
    )

    # Delete one permit.
    deleted_id = int(updated.iloc[1]["_id"])
    updated = updated.drop(updated.index[1])

    # Insert one new permit.
    new_row = updated.iloc[0].copy()
    new_id = int(updated["_id"].max()) + 1_000_000
    new_row["_id"] = new_id
    new_row["PERMIT_TYPE"] = "SCD2_TEST_INSERTED"

    updated = pd.concat(
        [updated, pd.DataFrame([new_row])],
        ignore_index=True,
    )

    # Preserve the original Arrow schema and avoid pandas index columns.
    updated_table = pa.Table.from_pandas(
        updated,
        schema=source_table.schema,
        preserve_index=False,
        safe=False,
    )

    original_file = tmp_path / "original.parquet"
    updated_file = tmp_path / "updated.parquet"
    database = tmp_path / "test.duckdb"

    pq.write_table(
        pa.Table.from_pandas(
            source_df,
            schema=source_table.schema,
            preserve_index=False,
            safe=False,
        ),
        original_file,
    )

    pq.write_table(updated_table, updated_file)

    print("    changed: 1")
    print("    deleted: 1")
    print("    inserted: 1")

    config = load_config(PROJECT_ROOT / "open_data_config.yml")

    source = next(
        source for source in config.sources if source.name == "active_permits"
    )

    # Temporary technical key used only to test SCD2 mechanics.
    source = replace(
        source,
        warehouse_load=True,
        primary_key=("_id",),
    )

    print("[3/6] Loading original snapshot...")

    load_scd2(
        credentials=str(database),
        snapshot_path=original_file,
        source=source,
        dataset_name="raw",
        destination_kind="duckdb",
    )

    print("    original snapshot loaded")

    print("[4/6] Loading changed snapshot...")

    load_scd2(
        credentials=str(database),
        snapshot_path=updated_file,
        source=source,
        dataset_name="raw",
        destination_kind="duckdb",
    )

    print("    changed snapshot loaded")

    print("[5/6] Reloading unchanged snapshot...")

    load_scd2(
        credentials=str(database),
        snapshot_path=updated_file,
        source=source,
        dataset_name="raw",
        destination_kind="duckdb",
    )

    print("    unchanged snapshot reloaded")

    print("[6/6] Validating SCD2 history...")

    with duckdb.connect(str(database), read_only=True) as connection:
        total = connection.execute(
            "SELECT COUNT(*) FROM raw.raw_active_permits"
        ).fetchone()[0]

        current = connection.execute(
            """
            SELECT COUNT(*)
            FROM raw.raw_active_permits
            WHERE _dlt_valid_to IS NULL
            """
        ).fetchone()[0]

        changed_versions = connection.execute(
            """
            SELECT COUNT(*)
            FROM raw.raw_active_permits
            WHERE _id = ?
            """,
            [changed_id],
        ).fetchone()[0]

        deleted_versions = connection.execute(
            """
            SELECT COUNT(*)
            FROM raw.raw_active_permits
            WHERE _id = ?
            """,
            [deleted_id],
        ).fetchone()[0]

        deleted_current = connection.execute(
            """
            SELECT COUNT(*)
            FROM raw.raw_active_permits
            WHERE _id = ?
              AND _dlt_valid_to IS NULL
            """,
            [deleted_id],
        ).fetchone()[0]

        inserted_versions = connection.execute(
            """
            SELECT COUNT(*)
            FROM raw.raw_active_permits
            WHERE _id = ?
            """,
            [new_id],
        ).fetchone()[0]

    print(f"    total historical rows: {total}")
    print(f"    current rows: {current}")
    print(f"    changed permit versions: {changed_versions}")
    print(f"    deleted permit versions: {deleted_versions}")
    print(f"    deleted current versions: {deleted_current}")
    print(f"    inserted permit versions: {inserted_versions}")

    # 10 original rows
    # +1 new version for the changed row
    # +1 inserted row
    # = 12 historical rows.
    assert total == 12, (
        f"SCD2 history incorrect: expected 12, got {total}. "
        f"changed={changed_versions}, "
        f"deleted={deleted_versions}, "
        f"inserted={inserted_versions}"
    )

    # 9 original permits remain current + 1 inserted permit.
    assert current == 10, f"Current-row count incorrect: expected 10, got {current}"

    assert changed_versions == 2, (
        f"Changed permit should have 2 versions, got {changed_versions}"
    )

    assert deleted_versions == 1, (
        f"Deleted permit should retain 1 historical version, got {deleted_versions}"
    )

    assert deleted_current == 0, (
        f"Deleted permit should have 0 current versions, got {deleted_current}"
    )

    assert inserted_versions == 1, (
        f"Inserted permit should have 1 version, got {inserted_versions}"
    )

    print("    SCD2 validation passed")
