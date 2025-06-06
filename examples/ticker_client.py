import argparse
import time
from threading import Event
import numpy as np
import raftmem

parser = argparse.ArgumentParser()
parser.add_argument('--server', default='0.0.0.0:7010')
parser.add_argument('--tickers', default='AAPL,MSFT,GOOG')
parser.add_argument('--rows', type=int, default=1000)
args = parser.parse_args()

symbols = args.tickers.split(',')
num_tickers = len(symbols)
num_rows = args.rows

update_event = Event()


def on_update(data):
    symbol = data.get('symbol')
    row = data.get('row')
    if symbol is not None and row is not None:
        try:
            idx = symbols.index(symbol)
        except ValueError:
            print('unknown symbol', symbol)
        else:
            price = arr[row, idx]
            print(f"{symbol} row {row}: {price}")
    else:
        print('update', data)
    update_event.set()

arr = None  # will hold ndarray view once node is started
node = raftmem.start('client', server=args.server, shape=[num_rows * num_tickers], on_update=on_update)
arr = node.ndarray.reshape(num_rows, num_tickers)

while True:
    update_event.wait()
    update_event.clear()
    with node.read():
        means = np.mean(arr, axis=0)
    print({sym: m for sym, m in zip(symbols, means)})
