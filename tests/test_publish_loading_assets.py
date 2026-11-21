from __future__ import annotations

import shutil

import pytest

from scripts import publish_loading_assets as publisher


class FakeReleaseClient:
    def __init__(self, state="missing", assets=None):
        self.release_state = state
        self.assets = dict(assets or {})
        self.uploads = []

    def state(self, tag):
        assert tag == publisher.RELEASE_TAG
        return self.release_state

    def download(self, tag, directory):
        assert tag == publisher.RELEASE_TAG
        shutil.rmtree(directory, ignore_errors=True)
        directory.mkdir(parents=True)
        for name, data in self.assets.items():
            (directory / name).write_bytes(data)

    def create(self, tag, paths, target):
        assert tag == publisher.RELEASE_TAG
        self.release_state = "draft"
        self.assets = {path.name: path.read_bytes() for path in paths}

    def upload(self, tag, path):
        assert tag == publisher.RELEASE_TAG
        self.uploads.append(path.name)
        self.assets[path.name] = path.read_bytes()

    def edit(self, tag):
        assert tag == publisher.RELEASE_TAG
        self.release_state = "published"

    def delete(self, tag):
        assert tag == publisher.RELEASE_TAG
        self.release_state = "missing"
        self.assets.clear()


def _candidate_assets(tmp_path):
    paths = {}
    for name in publisher.ASSETS:
        path = tmp_path / name
        path.write_bytes(("candidate:" + name).encode())
        paths[name] = path
    return paths


def test_initial_loading_release_publishes_and_verifies_all_three_assets(tmp_path):
    client = FakeReleaseClient()
    paths = _candidate_assets(tmp_path)
    publisher.publish_assets(client, paths, tmp_path / "backup")
    assert client.release_state == "published"
    assert set(client.assets) == set(publisher.ASSETS)
    for name, path in paths.items():
        assert client.assets[name] == path.read_bytes()


def test_loading_release_rolls_back_all_assets_after_partial_update(tmp_path):
    original = {name: ("original:" + name).encode() for name in publisher.ASSETS}
    client = FakeReleaseClient("published", original)
    paths = _candidate_assets(tmp_path)
    backup_dir = tmp_path / "backup"
    with pytest.raises(RuntimeError, match="restored and verified"):
        publisher.publish_assets(
            client, paths, backup_dir, simulate_failure_after_upload=2
        )
    assert client.release_state == "published"
    assert client.assets == original

    retained_backup = backup_dir / "last-known-good"
    assert {
        name: (retained_backup / name).read_bytes() for name in publisher.ASSETS
    } == original
