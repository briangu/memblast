import argparse
import asyncio
from quart import Quart, websocket
import memblast

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
    ws.onmessage = (e) => { document.body.innerHTML = '<pre>' + e.data + '</pre>'; };
    </script>
    </body></html>
    """

async def main(node):
    await asyncio.Event().wait()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    node = memblast.start('web', server=args.server, shape=[10,10], main=main, on_update=handle_update, event_loop=loop)
    loop.create_task(app.run_task(port=args.port))
    loop.run_forever()
