import argparse
import time
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
arr = node.ndarray().reshape(window, len(tickers))
con.register('data', arr)

query = 'SELECT ' + ', '.join(f'AVG(column{i})' for i in range(len(tickers))) + ' FROM data'

while True:
    with node.read():
        means = con.execute(query).fetchone()
    print("\033[H\033[J", end="")
    for t, m in zip(tickers, means):
        print(f'{t}: {m:.2f}')
    sys.stdout.flush()
    time.sleep(1)
