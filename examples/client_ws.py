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
        print("\033[H\033[J", end="")
        print(arr)
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

async def main():
    memblast.start(
        'web', server=args.server, shape=[10, 10],
        on_update_async=handle_update,
        event_loop=asyncio.get_running_loop()
    )
    app_task = asyncio.create_task(app.run_task(port=args.port))

    # Wait for a shutdown signal
    stop = asyncio.Future()
    def shutdown():
        if not stop.done():
            stop.set_result(None)
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, shutdown)
    loop.add_signal_handler(signal.SIGTERM, shutdown)
    await stop

    # Optionally, clean up
    app_task.cancel()
    try:
        await app_task
    except asyncio.CancelledError:
        pass

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down gracefully...")
