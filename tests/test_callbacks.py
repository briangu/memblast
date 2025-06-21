import asyncio
import threading
import time
import memblast


def start_loop():
    loop = asyncio.new_event_loop()
    t = threading.Thread(target=loop.run_forever)
    t.start()
    return loop, t


def stop_loop(loop, thread):
    loop.call_soon_threadsafe(loop.stop)
    thread.join()


def test_connect_disconnect_callbacks(free_tcp_port):
    loop, t = start_loop()
    events = []

    async def srv_connect(node, sub):
        events.append(("srv_connect", sub["name"]))

    async def srv_disconnect(node, peer):
        events.append(("srv_disconnect", peer))

    async def cli_connect(node, sub):
        events.append(("cli_connect", sub["name"]))

    async def cli_disconnect(node, peer):
        events.append(("cli_disconnect", peer))

    addr = f"127.0.0.1:{free_tcp_port}"

    server = memblast.start(
        "srv",
        listen=addr,
        shape=[1],
        on_connect=srv_connect,
        on_disconnect=srv_disconnect,
        event_loop=loop,
    )

    client = memblast.start(
        "cli",
        server=addr,
        shape=[1],
        on_connect=cli_connect,
        on_disconnect=cli_disconnect,
        event_loop=loop,
    )

    time.sleep(1)

    assert ("srv_connect", "cli") in events
    assert ("cli_connect", "cli") in events

    del client
    time.sleep(1)

    del server
    stop_loop(loop, t)
