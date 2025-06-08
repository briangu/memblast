import argparse
import asyncio
import memblast
import duckdb

parser = argparse.ArgumentParser()
parser.add_argument("--server", default="0.0.0.0:7020")
parser.add_argument("--tickers", default="2,50,75")
parser.add_argument("--window", type=int, default=5)
args = parser.parse_args()

tickers = [int(t) for t in args.tickers.split(",") if t]

maps = []
for i, t in enumerate(tickers):
    maps.append(([t, 0], [1, args.window], [i, 0], None))

con = duckdb.connect()

async def handle_update(node, meta):
    with node.read() as arr:
        arr = arr.reshape(len(tickers), args.window)
        print(arr.shape)
        result = con.execute(
            "SELECT AVG(column0), AVG(column1), AVG(column2) FROM data"
        ).fetchall()[0]
        print("\033[H\033[J", end="")
        print(result)

# Node will be provided to main; create array after start.
async def main(node):
    arr = node.ndarray().reshape(len(tickers), args.window)
    con.register("data", arr)
    await asyncio.Event().wait()

memblast.start(
    "slice_duckdb_client",
    server=args.server,
    shape=[len(tickers), args.window],
    maps=maps,
    main=main,
    on_update=handle_update,
)
