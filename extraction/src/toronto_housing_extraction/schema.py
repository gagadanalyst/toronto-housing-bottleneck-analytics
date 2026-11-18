"""Explicit source schema and normalization for Toronto Building Permits."""

from __future__ import annotations

from typing import Any

import pandas as pd

# These types are our extraction contract, not types inferred from CKAN.
# Dates intentionally remain strings to preserve their original representation.
SOURCE_FIELD_TYPES: dict[str, str] = {
    "_id": "int4",
    "PERMIT_NUM": "text",
    "REVISION_NUM": "text",
    "PERMIT_TYPE": "text",
    "STRUCTURE_TYPE": "text",
    "WORK": "text",
    "STREET_NUM": "text",
    "STREET_NAME": "text",
    "STREET_TYPE": "text",
    "STREET_DIRECTION": "text",
    "POSTAL": "text",
    "GEO_ID": "text",
    "WARD_GRID": "text",
    "APPLICATION_DATE": "date",
    "ISSUED_DATE": "date",
    "COMPLETED_DATE": "date",
    "STATUS": "text",
    "DESCRIPTION": "text",
    "CURRENT_USE": "text",
    "PROPOSED_USE": "text",
    "DWELLING_UNITS_CREATED": "text",
    "DWELLING_UNITS_LOST": "text",
    "EST_CONST_COST": "text",
    "ASSEMBLY": "float8",
    "INSTITUTIONAL": "float8",
    "RESIDENTIAL": "float8",
    "BUSINESS_AND_PERSONAL_SERVICES": "float8",
    "MERCANTILE": "float8",
    "INDUSTRIAL": "float8",
    "INTERIOR_ALTERATIONS": "float8",
    "DEMOLITION": "float8",
    "BUILDER_NAME": "text",
}


SOURCE_DATE_FIELDS = tuple(
    name for name, field_type in SOURCE_FIELD_TYPES.items() if field_type == "date"
)

SOURCE_TEXT_FIELDS = frozenset(
    name
    for name, field_type in SOURCE_FIELD_TYPES.items()
    if field_type in {"text", "date"}
)


def source_csv_dtypes() -> dict[str, str]:
    """Prevent Pandas from inferring types for text and date fields."""
    return {column: "string" for column in SOURCE_TEXT_FIELDS}


def validate_source_dates(
    dataframe: pd.DataFrame,
    source_name: str,
) -> None:
    """Validate date values without changing their original representation."""
    actual = {str(column).lower(): column for column in dataframe.columns}

    for configured in SOURCE_DATE_FIELDS:
        column = actual.get(configured.lower())
        if column is None:
            continue

        values = dataframe.loc[dataframe[column].notna(), column]
        parsed = pd.to_datetime(values, errors="coerce", format="mixed")

        if parsed.isna().any():
            raise ValueError(f"{source_name}: {column} contains unparseable dates.")


def _source_text_value(
    value: Any,
    column: str,
    declared_type: str,
) -> Any:
    """Preserve text exactly and reject potentially lossy numeric coercion."""
    if value is None or value is pd.NA:
        return pd.NA

    try:
        if pd.isna(value):
            return pd.NA
    except TypeError, ValueError:
        pass

    if isinstance(value, str):
        return pd.NA if value == "" else value

    raise ValueError(
        f"CKAN field {column!r} is declared {declared_type} but received "
        f"a non-string value ({type(value).__name__}: {value!r}); "
        "refusing lossy coercion. A numeric JSON value may already "
        "have lost leading zeroes."
    )


def normalize_source_schema(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize known fields consistently across DataStore, CSV and JSON."""
    frame = frame.copy()
    actual_columns = {str(column).upper(): column for column in frame.columns}

    for wanted, declared_type in SOURCE_FIELD_TYPES.items():
        column = actual_columns.get(wanted.upper())
        if column is None:
            continue

        if declared_type in {"text", "date"}:
            values = [
                _source_text_value(value, str(column), declared_type)
                for value in frame[column].tolist()
            ]
            frame[column] = pd.Series(
                values,
                index=frame.index,
                dtype="string",
            )

        elif declared_type == "float8":
            series = frame[column].replace("", pd.NA)
            try:
                converted = pd.to_numeric(series, errors="raise")
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"CKAN field {column!r} is declared float8 "
                    "but contains non-numeric values."
                ) from exc

            frame[column] = converted.astype("float64")

        elif declared_type in {"int2", "int4", "int8"}:
            series = frame[column].replace("", pd.NA)
            try:
                converted = pd.to_numeric(series, errors="raise")
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"CKAN field {column!r} is declared {declared_type} "
                    "but contains non-numeric values."
                ) from exc

            non_null = converted.dropna()
            if not (non_null % 1 == 0).all():
                raise ValueError(
                    f"CKAN field {column!r} is declared {declared_type} "
                    "but contains fractional values."
                )

            integer_dtype = {
                "int2": "Int16",
                "int4": "Int32",
                "int8": "Int64",
            }[declared_type]
            frame[column] = converted.astype(integer_dtype)

    # Keep known fields in a stable order and retain unknown fields for drift review.
    actual_columns = {str(column).upper(): column for column in frame.columns}
    canonical = [
        actual_columns[name.upper()]
        for name in SOURCE_FIELD_TYPES
        if name.upper() in actual_columns
    ]
    known_names = {name.upper() for name in SOURCE_FIELD_TYPES}
    unknown = [
        column for column in frame.columns if str(column).upper() not in known_names
    ]

    return frame.loc[:, canonical + unknown]
