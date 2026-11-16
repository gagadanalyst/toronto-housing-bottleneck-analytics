"""Command-line interface for Toronto Housing Extraction."""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
import uuid
from pathlib import Path

from .ckan_client import TorontoCKANClient
from .config import load_config
from .extraction import extract_sources
from .snapshots import create_snapshot_bundle

LOGGER = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Build the extraction command-line interface."""
    parser = argparse.ArgumentParser(
        description=(
            "Extract Toronto Open Data and produce validated Parquet snapshots."
        )
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/sources.yml"),
        help="Path to the extraction source configuration.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/extraction"),
        help="Directory for extraction staging and immutable snapshots.",
    )
    return parser


def main() -> int:
    """Run one complete paired-source extraction."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        stream=sys.stderr,
    )
    args = build_parser().parse_args()

    run_dir: Path | None = None

    try:
        LOGGER.info("Toronto Housing Extraction")
        LOGGER.info("----------------------------")
        LOGGER.info("[1/4] Loading configuration...")

        config = load_config(args.config)

        LOGGER.info("      Loaded %d configured sources.", len(config.sources))
        LOGGER.info("[2/4] Preparing Toronto CKAN client...")

        snapshot_id = uuid.uuid4().hex

        staging_root = args.output / "staging"
        snapshot_root = args.output / "snapshots"

        run_dir = staging_root / snapshot_id
        bundle_path = snapshot_root / f"{snapshot_id}.zip"

        run_dir.mkdir(parents=True, exist_ok=False)
        snapshot_root.mkdir(parents=True, exist_ok=True)

        with TorontoCKANClient(
            base_url=config.ckan.base_url,
            timeout_seconds=config.ckan.timeout_seconds,
            retries=config.ckan.retries,
        ) as client:
            LOGGER.info("[3/4] Extracting configured sources...")
            extractions = extract_sources(
                client=client,
                config=config,
                run_dir=run_dir,
            )
        LOGGER.info("[4/4] Packaging and verifying snapshot...")
        bundle = create_snapshot_bundle(
            extractions,
            config,
            bundle_path,
            snapshot_id=snapshot_id,
        )
        LOGGER.info("    Snapshot archive verified.")

    except Exception as exc:
        print(f"toronto-extract: {exc}", file=sys.stderr)

        if run_dir is not None and run_dir.exists():
            print(
                f"Extraction staging retained at: {run_dir}",
                file=sys.stderr,
            )

        return 1

    try:
        shutil.rmtree(run_dir)
    except OSError as exc:
        LOGGER.warning("Warning: snapshot created, but staging cleanup failed: %s", exc)

    print("Snapshot created successfully.")
    print(f"Snapshot ID: {bundle['snapshot_id']}")

    for source_name, source in bundle["sources"].items():
        print(f"{source_name}: {source['row_count']:,} rows")

    print(f"Archive: {bundle_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
