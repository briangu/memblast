
import argparse
import numpy as np
import memblast
import sys
import duckdb


async def handle_update(node, meta):
    global latest_idx, tickers, window, con, query_template, initialized
    latest_idx = meta.get('index', latest_idx)
    if not initialized:
        data = node.ndarray().reshape(len(tickers), window)
        con.register('data', data)
        initialized = True
    with node.read() as arr:
        data = np.array(arr).reshape(len(tickers), window)
        valid = min(latest_idx + 1, window)
        means = (
            con.execute(query_template.format(limit=valid)).fetchall()[0]
            if valid > 0
            else [0.0] * len(tickers)
        )
        print("\033[H\033[J", end="")
        for t, m in zip(tickers, means):
            print(f'{t}: {m:.2f}')
        sys.stdout.flush()

parser = argparse.ArgumentParser()
parser.add_argument('--server', default='0.0.0.0:7011')
parser.add_argument('--tickers', default='AAPL,GOOG,MSFT')
parser.add_argument('--window', type=int, default=390,
                    help='Size of the history buffer (minutes in a trading day)')
args = parser.parse_args()

tickers = args.tickers.split(',')
window = args.window
latest_idx = -1

con = duckdb.connect()
query_template = 'SELECT ' + ', '.join(f'AVG(column{i})' for i in range(len(tickers))) + ' FROM (SELECT * FROM data LIMIT {limit})'
initialized = False

node = memblast.start(
    "ticker_client",
    server=args.server,
    shape=[len(tickers), window],
    on_update_async=handle_update,
)

