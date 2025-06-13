import argparse
import asyncio
from quart import Quart, websocket
import memblast
import signal

parser = argparse.ArgumentParser()
parser.add_argument('--server', default='0.0.0.0:7010')
parser.add_argument('--port', type=int, default=5000)
args = parser.parse_args()

app = Quart(__name__)
clients = set()

async def handle_update(node, meta):
    with node.read() as arr:
        data = str(arr.tolist())
    for ws in list(clients):
        await ws.send(data)

@app.websocket('/ws')
async def ws_endpoint():
    clients.add(websocket._get_current_object())
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        clients.remove(websocket._get_current_object())

@app.route('/')
async def index():
    return """
    <html><body>
    <script>
    const ws = new WebSocket('ws://' + location.host + '/ws');
    ws.onmessage = (e) => { document.body.innerHTML = e.data; };
    </script>
    </body></html>
    """

async def main() -> None:
    loop   = asyncio.get_running_loop()
    stop   = asyncio.Event()

    memblast.start(
        "web", server=args.server, shape=[10, 10],
        on_update=handle_update, event_loop=loop,
    )

    def _arm_shutdown(sig: signal.Signals):
        loop.create_task(_shutdown(sig))

    async def _shutdown(sig):
        print(f"\n💡 received {sig.name}, shutting down …")
        stop.set()                        # 1) unblock Quart
        await asyncio.gather(
            *(ws.close(code=1001, reason="server shutdown") for ws in clients),
            return_exceptions=True,
        )

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _arm_shutdown, sig)

    await app.run_task(port=args.port, shutdown_trigger=stop.wait)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n💡 KeyboardInterrupt → shutdown request already handled.")
