import argparse
import random
import time
import memblast
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--listen', default='0.0.0.0:7011')
parser.add_argument('--tickers', default='AAPL,GOOG,MSFT')
parser.add_argument('--window', type=int, default=390,
                    help='Size of the history buffer (minutes in a trading day)')
args = parser.parse_args()

tickers = args.tickers.split(',')
window = args.window
node = memblast.start("ticker_server", listen=args.listen, shape=[len(tickers), window])

index = 0
while True:
    with node.write() as arr:
        if index < window:
            for i in range(len(tickers)):
                arr[i, index] = random.uniform(100.0, 200.0)
        node.version_meta({'index': index})
    with node.read() as data:
        print("\033[H\033[J", end="")
        for t, row in zip(tickers, data):
            print(t, row)
        sys.stdout.flush()
    time.sleep(1)
    index += 1
