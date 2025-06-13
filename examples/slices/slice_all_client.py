import argparse
import time
import memblast
import sys
import asyncio
import threading

parser = argparse.ArgumentParser()
parser.add_argument('--server', default='0.0.0.0:7020')
args = parser.parse_args()

async def handle_update(_node, meta):
    print("metadata", meta)


loop = asyncio.new_event_loop()
t = threading.Thread(target=loop.run_forever)
t.start()

node = memblast.start("b", server=args.server, shape=[100,5], on_update=handle_update, event_loop=loop)

while True:
    with node.read() as arr:
        print("\033[H\033[J", end="")  # Move cursor to home position and clear screen
        print(arr)
        sys.stdout.flush()
    time.sleep(1)

