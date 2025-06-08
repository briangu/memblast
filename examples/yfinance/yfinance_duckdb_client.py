import argparse
import asyncio
import memblast
import duckdb
import numpy as np
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--server', default='0.0.0.0:7012')
parser.add_argument('--tickers', default='AAPL,GOOG,MSFT')
parser.add_argument('--window', type=int, default=5)
args = parser.parse_args()

tickers = [t.strip() for t in args.tickers.split(',') if t.strip()]
window = args.window
node = memblast.start('yfinance_duck_client', server=args.server, shape=[len(tickers), window])

con = duckdb.connect()
arr = node.ndarray().reshape(len(tickers), window)
con.register('data', arr)

query = 'SELECT ' + ', '.join(f'AVG(column{i})' for i in range(len(tickers))) + ' FROM data'


async def handle_update(meta):
    print("\033[H\033[J", end="")
    with node.read() as arr:
        arr = arr.reshape(len(tickers), window)
        means = con.execute(query).fetchall()[0]
        for i, (t, m) in enumerate(zip(tickers, means)):
            print(f'{t}: data: {arr[i]} mean: {m:.2f}')
        sys.stdout.flush()


node.on_update_async(handle_update)

asyncio.get_event_loop().run_forever()
