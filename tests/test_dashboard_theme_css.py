from __future__ import annotations

from pathlib import Path

APP_SOURCE = Path(__file__).resolve().parents[1] / "dashboard" / "app.py"


def test_main_widget_labels_use_live_streamlit_theme_variables() -> None:
    source = APP_SOURCE.read_text(encoding="utf-8")

    assert "--app-text: var(--text-color, {TEXT_COLOR});" in source
    assert '[data-testid="stMain"] [data-testid="stWidgetLabel"]' in source
    assert '[data-testid="stMain"] [data-testid="stSlider"] label' in source
    assert ".metric-value {{ color: var(--app-text);" in source


def test_theme_palette_is_detected_for_python_rendered_figures() -> None:
    source = APP_SOURCE.read_text(encoding="utf-8")

    assert "THEME_TYPE = current_theme_type()" in source
