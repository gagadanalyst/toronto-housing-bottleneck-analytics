from toronto_housing_ingestion.ingestion import build_parser


def test_source_option_can_be_repeated() -> None:
    args = build_parser().parse_args(
        ["--source", "active_permits", "--source", "cleared_permits"]
    )
    assert args.source == ["active_permits", "cleared_permits"]
