from __future__ import annotations

from click import ClickException

from plextraktsync.factory import factory
from plextraktsync.watch.broker import WatchBroker
from plextraktsync.watch.events import Error, PlaySessionStateNotification, ServerStarted


def watch_broker(server: str):
    factory.run_config.update(
        server=server,
    )
    ws = factory.web_socket_listener
    broker = WatchBroker(
        plex=factory.plex_api,
        updater=factory.watch_state_updater,
        config=factory.config,
    )

    try:
        broker.validate()
    except RuntimeError as e:
        raise ClickException(str(e))

    ws.on(ServerStarted, broker.on_start)
    ws.on(
        PlaySessionStateNotification,
        broker.on_play,
        state=["playing", "stopped", "paused"],
    )
    ws.on(Error, broker.on_error)

    ws.listen()
