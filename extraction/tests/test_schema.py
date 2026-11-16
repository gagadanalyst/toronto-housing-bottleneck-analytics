import pandas as pd
import pytest

from toronto_housing_extraction.schema import normalize_source_schema


def test_normalizes_fields_without_losing_original_values():
    frame = pd.DataFrame(
        {
            "PERMIT_NUM": ["001234"],
            "REVISION_NUM": ["00"],
            "GEO_ID": ["00001"],
            "APPLICATION_DATE": ["2024-01-15"],
            "ASSEMBLY": ["125.50"],
            "NEW_SOURCE_FIELD": ["preserved"],
        }
    )

    result = normalize_source_schema(frame)

    assert result["PERMIT_NUM"].iloc[0] == "001234"
    assert result["REVISION_NUM"].iloc[0] == "00"
    assert result["GEO_ID"].iloc[0] == "00001"
    assert result["APPLICATION_DATE"].iloc[0] == "2024-01-15"
    assert result["ASSEMBLY"].iloc[0] == 125.50
    assert result["NEW_SOURCE_FIELD"].iloc[0] == "preserved"

    assert result["ASSEMBLY"].dtype == "float64"
    assert result["PERMIT_NUM"].dtype == "string"


def test_rejects_numeric_values_for_text_fields():
    frame = pd.DataFrame({"GEO_ID": [12345]})

    with pytest.raises(ValueError, match="GEO_ID"):
        normalize_source_schema(frame)


def test_rejects_fractional_values_for_integer_fields():
    frame = pd.DataFrame({"_id": [1.5]})

    with pytest.raises(ValueError, match="fractional"):
        normalize_source_schema(frame)
