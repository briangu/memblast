import argparse
import asyncio
import numpy as np
import memblast
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--server", default="0.0.0.0:7011")
parser.add_argument("--tickers", default="AAPL,GOOG,MSFT")
parser.add_argument("--window", type=int, default=5)
args = parser.parse_args()

tickers = args.tickers.split(",")
window = args.window
node = memblast.start("ticker_client", server=args.server, shape=[len(tickers), window])


async def handle_update(meta):
    print("metadata", meta)
    with node.read() as arr:
        data = np.array(arr).reshape(len(tickers), window)
        means = data.mean(axis=1)
        print("\033[H\033[J", end="")
        for t, m in zip(tickers, means):
            print(f"{t}: {m:.2f}")
        sys.stdout.flush()


async def main():
    node.on_update_async(handle_update)
    await asyncio.Event().wait()


asyncio.run(main())
