
import argparse
import time
import numpy as np
import memblast
import sys
import duckdb

parser = argparse.ArgumentParser()
parser.add_argument('--server', default='0.0.0.0:7011')
parser.add_argument('--tickers', default='AAPL,GOOG,MSFT')
parser.add_argument('--window', type=int, default=5)
args = parser.parse_args()

tickers = args.tickers.split(',')
window = args.window
node = memblast.start("ticker_client", server=args.server, shape=[len(tickers), window])


def handle_update(meta):
    print('metadata', meta)


node.on_update(handle_update)

con = duckdb.connect()
# Register the numpy array ONCE. This is a live view into the shared memory,
# so DuckDB will always see the latest data without re-registering.
arr = node.ndarray()
arr = arr.reshape([3,window])
con.register('data', arr)

while True:
    with node.read() as arr:
        data = np.array(arr).reshape(len(tickers), window)
        means = con.execute('SELECT AVG(column0), AVG(column1), AVG(column2) FROM data').fetchall()[0]
        print("\033[H\033[J", end="")
        for t, m in zip(tickers, means):
            print(f'{t}: {m:.2f}')
        sys.stdout.flush()
    time.sleep(1)

