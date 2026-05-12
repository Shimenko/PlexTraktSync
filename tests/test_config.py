#!/usr/bin/env python3 -m pytest
from __future__ import annotations

from os.path import join

import pytest

from plextraktsync.config.Config import Config
from plextraktsync.factory import factory


def test_config_merge():
    config = factory.config

    override = {"root": {"key1": "value1"}}
    config.merge(override, config)
    override = {"root": {"key2": "value2"}}
    config.merge(override, config)
    assert config["root"]["key1"] == "value1"
    assert config["root"]["key2"] == "value2"


def test_config_merge_real():
    from tests.conftest import MOCK_DATA_DIR

    config_file = join(MOCK_DATA_DIR, "673-config.yml")
    config = Config(config_file)

    assert config["sync"]["plex_to_trakt"]["collection"] is False


@pytest.mark.skip(reason="Broken in CI")
def test_sync_config():
    from tests.conftest import MOCK_DATA_DIR

    config_file = join(MOCK_DATA_DIR, "673-config.yml")
    sync_config = Config(config_file).sync

    assert sync_config.plex_to_trakt["collection"] is False


def test_http_config():
    from tests.conftest import MOCK_DATA_DIR

    config = Config()
    config.config_yml = join(MOCK_DATA_DIR, "http_cache-blank.yml")
    assert config.http_cache is not None

    config = Config()
    config.config_yml = join(MOCK_DATA_DIR, "http_cache-empty.yml")
    assert config.http_cache is not None

    config = Config()
    config.config_yml = join(MOCK_DATA_DIR, "http_cache-1-entry.yml")
    cache = config.http_cache
    assert cache is not None
    assert cache.policy["a"] == "b"


def test_trakt_rate_limit_config_default():
    from tests.conftest import MOCK_DATA_DIR

    config = Config()
    config.config_yml = join(MOCK_DATA_DIR, "http_cache-blank.yml")

    assert config.trakt_rate_limit.get_delay == 1.0


def test_trakt_rate_limit_config_override(tmp_path):
    config_file = tmp_path / "config.yml"
    config_file.write_text("trakt:\n  rate_limit:\n    get_delay: 2.5\n")
    config = Config(str(config_file))

    assert config.trakt_rate_limit.get_delay == 2.5


def test_trakt_rate_limit_config_rejects_non_positive_delay(tmp_path):
    config_file = tmp_path / "config.yml"
    config_file.write_text("trakt:\n  rate_limit:\n    get_delay: 0\n")
    config = Config(str(config_file))

    with pytest.raises(ValueError, match="trakt.rate_limit.get_delay must be a positive number"):
        _ = config.trakt_rate_limit
