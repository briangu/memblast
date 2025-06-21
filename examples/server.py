import argparse
import random
import time
import memblast
import sys
import asyncio

parser = argparse.ArgumentParser()
parser.add_argument('--listen', default='0.0.0.0:7010')
args = parser.parse_args()

async def on_connect(node, sub):
    print('connect', sub['name'])


async def on_disconnect(node, peer):
    print('disconnect', peer)

async def on_update(node):
    print('update', node.version)

loop = asyncio.get_event_loop()

node = memblast.start(
    "a",
    listen=args.listen,
    shape=[10, 10],
    on_update_async=on_update,
    on_connect=on_connect,
    on_disconnect=on_disconnect,
    event_loop=loop,
)

async def main():
    while True:
        with node.write() as a:
            last = 0
            for _ in range(3):
                idx = random.randrange(len(a))
                a[idx] = random.random()
            last = idx
            node.version_meta({"last_index": last})
        with node.read() as arr:
            print("\033[H\033[J", end="")
            print(f"version: {node.version.get(node.name, 0)}")
            print(arr)
            sys.stdout.flush()
        await asyncio.sleep(1)

loop.run_until_complete(main())
loop.close()
