import argparse
import asyncio
import random
import numpy as np
import memblast
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--listen', default='0.0.0.0:7010')
args = parser.parse_args()

loop = asyncio.get_event_loop()
node = memblast.start("a", listen=args.listen, shape=[10,10], event_loop=loop)


async def main():
    while True:
        with node.write() as a:
            last = 0
            for _ in range(3):
                idx = random.randrange(len(a))
                a[idx] = random.random()
                last = idx
            node.send_meta({"last_index": last})
        with node.read() as arr:
            print("\033[H\033[J", end="")
            print(arr)
            sys.stdout.flush()
        await asyncio.sleep(1)


loop.create_task(main())
loop.run_forever()
