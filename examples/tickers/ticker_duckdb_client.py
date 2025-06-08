import argparse
import asyncio
import numpy as np
import memblast
import duckdb
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--server", default="0.0.0.0:7011")
parser.add_argument("--tickers", default="AAPL,GOOG,MSFT")
parser.add_argument("--window", type=int, default=5)
args = parser.parse_args()

tickers = args.tickers.split(",")
window = args.window

con = duckdb.connect()

async def handle_update(node, meta):
    with node.read() as arr:
        data = np.array(arr).reshape(len(tickers), window)
        means = con.execute(
            "SELECT AVG(column0), AVG(column1), AVG(column2) FROM data"
        ).fetchall()[0]
        print("\033[H\033[J", end="")
        for t, m in zip(tickers, means):
            print(f"{t}: {m:.2f}")
        sys.stdout.flush()

# Node passed to main for registration
async def main(node):
    arr = node.ndarray().reshape(len(tickers), window)
    con.register("data", arr)
    await asyncio.Event().wait()

memblast.start(
    "ticker_client",
    server=args.server,
    shape=[len(tickers), window],
    main=main,
    on_update=handle_update,
)
