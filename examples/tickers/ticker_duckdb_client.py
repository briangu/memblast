
import argparse
import time
import numpy as np
import memblast
import sys
import duckdb

parser = argparse.ArgumentParser()
parser.add_argument('--server', default='0.0.0.0:7011')
parser.add_argument('--tickers', default='AAPL,GOOG,MSFT')
parser.add_argument('--window', type=int, default=390,
                    help='Size of the history buffer (minutes in a trading day)')
args = parser.parse_args()

tickers = args.tickers.split(',')
window = args.window

node = memblast.start("ticker_client", server=args.server, shape=[len(tickers), window], on_update=handle_update)

latest_idx = -1

con = duckdb.connect()
# Register the numpy array ONCE. This is a live view into the shared memory,
# so DuckDB will always see the latest data without re-registering.
arr = node.ndarray().reshape(window, len(tickers))
con.register('data', arr)
query_template = 'SELECT ' + ', '.join(f'AVG(column{i})' for i in range(len(tickers))) + ' FROM (SELECT * FROM data LIMIT {limit})'

while True:
    with node.read() as arr:
        data = np.array(arr).reshape(len(tickers), window)
        valid = min(latest_idx + 1, window)
        means = con.execute(query_template.format(limit=valid)).fetchall()[0] if valid > 0 else [0.0]*len(tickers)
        print("\033[H\033[J", end="")
        for t, m in zip(tickers, means):
            print(f'{t}: {m:.2f}')
        sys.stdout.flush()
    time.sleep(1)

