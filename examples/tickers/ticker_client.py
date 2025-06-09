import argparse
import time
import numpy as np
import memblast
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--server', default='0.0.0.0:7011')
parser.add_argument('--tickers', default='AAPL,GOOG,MSFT')
parser.add_argument('--window', type=int, default=390,
                    help='Size of the history buffer (minutes in a trading day)')
args = parser.parse_args()

tickers = args.tickers.split(',')
window = args.window
def handle_update(meta):
    global latest_idx
    latest_idx = meta.get('index', latest_idx)


node = memblast.start("ticker_client", server=args.server, shape=[len(tickers), window], on_update=handle_update)

latest_idx = -1

while True:
    with node.read() as arr:
        data = np.array(arr).reshape(len(tickers), window)
        valid = min(latest_idx + 1, window)
        view = data[:, :valid] if valid > 0 else np.zeros((len(tickers), 0))
        means = view.mean(axis=1) if valid > 0 else np.zeros(len(tickers))
        print("\033[H\033[J", end="")
        for t, m in zip(tickers, means):
            print(f'{t}: {m:.2f}')
        sys.stdout.flush()
    time.sleep(1)

