import argparse
import numpy as np
import time
import memblast
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--peers', default='0.0.0.0:7020')
parser.add_argument('--tickers', default='2,50,75')
parser.add_argument('--window', type=int, default=5)
args = parser.parse_args()

tickers = [int(t) for t in args.tickers.split(',') if t]

maps = [([t, 0], [1, args.window], None, f'ticker_{t}') for t in tickers]

node = memblast.start('named_client', server=args.peers, shape=[1], maps=maps)

while True:
    with node.read():
        print("\033[H\033[J", end="")
        for t in tickers:
            arr = node.ndarray(f'ticker_{t}')
            if arr is not None:
                data = np.array(arr).reshape(1, args.window)
                print(f'{t}: {data[0]}')
        sys.stdout.flush()
    time.sleep(1)
