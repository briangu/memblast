import argparse
import asyncio
import memblast
import sys
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument("--server", default="0.0.0.0:7020")
parser.add_argument("--tickers", default="2,50,75")
parser.add_argument("--window", type=int, default=5)
args = parser.parse_args()

tickers = [int(t) for t in args.tickers.split(",") if t]

maps = [([t, 0], [1, args.window], None, f"ticker_{t}") for t in tickers]

node = memblast.start("named_client", server=args.server, shape=[1], maps=maps)


async def handle_update(meta):
    with node.read():
        print("\033[H\033[J", end="")
        for t in tickers:
            arr = node.ndarray(f"ticker_{t}")
            if arr is not None:
                data = np.array(arr).reshape(1, args.window)
                print(f"{t}: {data[0]}")
        sys.stdout.flush()


async def main():
    node.on_update_async(handle_update)
    await asyncio.Event().wait()


asyncio.run(main())
