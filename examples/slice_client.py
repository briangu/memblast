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

maps = []
for i, t in enumerate(tickers):
    maps.append(([t, 0], [1, args.window], [i, 0], None))

node = memblast.start('slice_client', server=args.peers, shape=[len(tickers), args.window], maps=maps)

while True:
    with node.read() as arr:
        arr = arr.reshape(len(tickers), args.window)
        print("\033[H\033[J", end="")
        for t, row in zip(tickers, arr):
            print(f'{t}: {row}')
        sys.stdout.flush()
    time.sleep(1)
