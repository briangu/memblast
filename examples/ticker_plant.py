import argparse
import random
import time
import numpy as np
import raftmem

parser = argparse.ArgumentParser()
parser.add_argument('--listen', default='0.0.0.0:7010')
parser.add_argument('--tickers', default='AAPL,MSFT,GOOG')
parser.add_argument('--rows', type=int, default=1000)
args = parser.parse_args()

symbols = args.tickers.split(',')
num_tickers = len(symbols)
num_rows = args.rows

# shared buffer sized for rows * tickers
node = raftmem.start('tp', listen=args.listen, shape=[num_rows * num_tickers])
arr = node.ndarray.reshape(num_rows, num_tickers)

# position for each ticker column
positions = [0 for _ in range(num_tickers)]

while True:
    # simulate tick for random ticker
    sym_index = random.randrange(num_tickers)
    price = random.random() * 100

    row = positions[sym_index]
    with node.write() as a:
        a[row * num_tickers + sym_index] = price
        positions[sym_index] = (row + 1) % num_rows
        a.update({'positions': positions, 'symbol': symbols[sym_index], 'row': row})
    time.sleep(0.1)
