from pathlib import Path

from toronto_housing_extraction.config import load_config


def test_loads_active_and_cleared_source_configuration():
    config = load_config(Path("config/sources.yml"))

    assert config.ckan.page_size == 10000
    assert config.ckan.retries == 4

    assert set(config.sources) == {
        "active_permits",
        "cleared_permits",
    }

    active = config.sources["active_permits"]

    assert active.dataset_slug == "building-permits-active-permits"
    assert active.business_key == (
        "PERMIT_NUM",
        "REVISION_NUM",
        "PERMIT_TYPE",
    )
    assert active.min_row_count == 1000
