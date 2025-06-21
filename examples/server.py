import argparse
import random
import threading
import time
import numpy as np
import asyncio
import memblast
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--listen', default='0.0.0.0:7010')
args = parser.parse_args()

loop = asyncio.new_event_loop()
t = threading.Thread(target=loop.run_forever, daemon=True)
t.start()


async def on_connect(node, sub):
    print('connect', sub['name'])


async def on_disconnect(node, peer):
    print('disconnect', peer)


node = memblast.start(
    "a",
    listen=args.listen,
    shape=[10, 10],
    on_connect=on_connect,
    on_disconnect=on_disconnect,
    event_loop=loop,
)

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
    time.sleep(1)
