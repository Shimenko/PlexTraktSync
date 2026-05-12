#!/usr/bin/env python3 -m pytest
from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace

from plextraktsync.plan.WalkConfig import WalkConfig
from plextraktsync.plan.Walker import Walker
from plextraktsync.plan.WalkPlanner import WalkPlanner
from plextraktsync.plex.PlexApi import PlexApi
from plextraktsync.plex.PlexLibrarySection import PlexLibrarySection


class PlexLibrarySectionMock(PlexLibrarySection):
    def __init__(self, data):
        self.data = data

    def find_by_title(self, name: str):
        items = [item for item in self.data["items"] if item["title"] == name]
        assert len(items) == 1
        return items[0]


class PlexMock(PlexApi):
    def __init__(self, sections):
        self.sections = sections

    def movie_sections(self, library=None):
        by_type = self.sections_by_type("movie", library)
        return by_type

    def show_sections(self, library=None):
        return self.sections_by_type("show", library)

    def sections_by_type(self, libtype, title):
        result = []
        for section in self.sections:
            if section["type"] != libtype:
                continue
            if title and section["title"] != title:
                continue
            result.append(PlexLibrarySectionMock(section))

        return result


class PagerSectionMock:
    title = "TV Shows"
    title_link = "TV Shows"

    def __init__(self, shows, episodes):
        self.shows = shows
        self.episodes = episodes

    def pager(self, libtype=None):
        if libtype == "episode":
            return self.episodes

        return self.shows


class LibraryItemMock:
    type = "show"

    def __init__(self, key, title):
        self.key = key
        self.title = title
        self.title_link = title


class EpisodeMock:
    type = "episode"

    def __init__(self, key, show_id, title):
        self.key = key
        self.show_id = show_id
        self.title = title
        self.title_link = title
        self.show = None


class MediaMock:
    def __init__(self, plex, show=None):
        self.plex = plex
        self.show = show


class MediaFactoryMock:
    def __init__(self):
        self.calls = []

    def resolve_any(self, item, show=None):
        self.calls.append((item, show))
        return MediaMock(item, show)


async def collect_episodes(walker):
    return [episode async for episode in walker.find_episodes()]


def test_walker():
    plex = PlexMock(
        [
            {
                "type": "movie",
                "title": "Movies",
                "items": [
                    {"title": "Batman Begins"},
                ],
            },
            {
                "type": "show",
                "title": "TV Shows",
                "items": [
                    {"title": "Breaking Bad"},
                ],
            },
        ]
    )

    wc = WalkConfig()
    wc.add_library("Movies")
    wc.add_movie("Batman Begins")
    wc.add_library("TV Shows")
    wc.add_show("Breaking Bad")
    plan = WalkPlanner(plex, wc).plan()

    assert len(plan.movie_sections) == 0
    assert len(plan.show_sections) == 0
    assert len(plan.movies) == 1
    assert len(plan.shows) == 1


def test_find_episodes_skips_parent_show_missing_from_preload(caplog):
    preloaded_show = LibraryItemMock(1, "Existing Show")
    existing_episode = EpisodeMock(10, preloaded_show.key, "Existing Episode")
    missing_episode = EpisodeMock(20, 999, "New Episode")
    duplicate_missing_episode = EpisodeMock(21, 999, "Another New Episode")
    section = PagerSectionMock(
        shows=[preloaded_show],
        episodes=[missing_episode, existing_episode, duplicate_missing_episode],
    )
    media_factory = MediaFactoryMock()
    walker = Walker(
        plex=SimpleNamespace(),
        trakt=SimpleNamespace(),
        mf=media_factory,
        config=SimpleNamespace(),
    )
    walker.__dict__["plan"] = SimpleNamespace(
        episodes=[],
        shows=[],
        show_sections=[section],
    )

    caplog.set_level(logging.WARNING)

    episodes = asyncio.run(collect_episodes(walker))

    assert len(episodes) == 1
    assert episodes[0].plex is existing_episode
    assert existing_episode.show is preloaded_show
    assert missing_episode.show is None
    assert duplicate_missing_episode.show is None
    assert [item for item, show in media_factory.calls] == [preloaded_show, existing_episode]
    assert "parent show 999 was not found in preloaded shows" in caplog.text
    assert caplog.text.count("parent show 999") == 1
