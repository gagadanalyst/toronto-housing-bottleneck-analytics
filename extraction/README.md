# Toronto Housing Extraction

Standalone Python application for extracting Toronto Open Data
and producing validated, immutable Parquet snapshots.

## Responsibilities

- Discover and inspect CKAN resources.
- Detect source changes.
- Extract Active and Cleared Building Permits.
- Normalize fields using an explicit schema.
- Validate Parquet files and source completeness.
- Produce snapshot bundles and manifests.

This application does not load or modify any warehouse.

## Development

Create a virtual environment:

    python -m venv .venv
    source .venv/bin/activate

Install the project:

    python -m pip install -e ".[dev]"

Check the command:

    toronto-extract --help
