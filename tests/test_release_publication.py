import importlib.util
import shutil
from pathlib import Path

import pytest

SCRIPT_PATH = (
    Path(__file__).resolve().parents[1] / "scripts" / "publish_release_assets.py"
)
SPEC = importlib.util.spec_from_file_location("publish_release_assets", SCRIPT_PATH)
if SPEC is None or SPEC.loader is None:
    raise ImportError(f"Could not load publication helper from {SCRIPT_PATH}")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
ASSET_NAMES = MODULE.ASSET_NAMES
publish_release_assets = MODULE.publish_release_assets


class FakeReleaseClient:
    def __init__(self, assets: dict[str, bytes], state: str = "published") -> None:
        self.assets = dict(assets)
        self.state = state
        self.edit_count = 0
        self.create_count = 0

    def release_state(self, tag: str) -> str:
        del tag
        return self.state

    def download(self, tag: str, directory: Path) -> None:
        del tag
        if self.state == "missing":
            raise RuntimeError("Release does not exist")
        shutil.rmtree(directory, ignore_errors=True)
        directory.mkdir(parents=True)
        for name, content in self.assets.items():
            (directory / name).write_bytes(content)

    def create_release(
        self, tag: str, paths: list[Path], title: str, notes: str, target: str
    ) -> None:
        del tag, title, notes, target
        self.assets = {path.name: path.read_bytes() for path in paths}
        self.state = "draft"
        self.create_count += 1

    def upload(self, tag: str, path: Path) -> None:
        del tag
        self.assets[path.name] = path.read_bytes()

    def edit(self, tag: str, title: str, notes: str) -> None:
        del tag, title, notes
        self.state = "published"
        self.edit_count += 1

    def delete(self, tag: str) -> None:
        del tag
        self.state = "missing"
        self.assets = {}


def _asset_fixtures(tmp_path: Path) -> tuple[dict[str, bytes], dict[str, Path]]:
    old_assets = {name: f"old:{name}".encode() for name in ASSET_NAMES}
    new_dir = tmp_path / "new-assets"
    new_dir.mkdir()
    new_paths = {}
    for name in ASSET_NAMES:
        path = new_dir / name
        path.write_bytes(f"new:{name}".encode())
        new_paths[name] = path
    return old_assets, new_paths


def test_partial_upload_failure_restores_last_known_good_assets(tmp_path: Path) -> None:
    old_assets, new_paths = _asset_fixtures(tmp_path)
    client = FakeReleaseClient(old_assets)
    with pytest.raises(RuntimeError, match="restored and verified"):
        publish_release_assets(
            client, new_paths, tmp_path / "recovery", simulate_failure_after_upload=1
        )
    assert client.assets == old_assets
    assert client.edit_count == 0


def test_successful_publication_replaces_all_assets(tmp_path: Path) -> None:
    old_assets, new_paths = _asset_fixtures(tmp_path)
    client = FakeReleaseClient(old_assets)
    publish_release_assets(client, new_paths, tmp_path / "recovery")
    assert client.assets == {
        name: path.read_bytes() for name, path in new_paths.items()
    }
    assert client.edit_count == 1
    assert not (tmp_path / "recovery").exists()


def test_initial_publication_creates_complete_release(tmp_path: Path) -> None:
    _, new_paths = _asset_fixtures(tmp_path)
    client = FakeReleaseClient({}, state="missing")
    publish_release_assets(client, new_paths, tmp_path / "recovery")
    assert client.state == "published"
    assert client.create_count == 1
    assert client.assets == {
        name: path.read_bytes() for name, path in new_paths.items()
    }
