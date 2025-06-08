import argparse
import random
import time
import memblast
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--listen', default='0.0.0.0:7020')
parser.add_argument('--tickers', type=int, default=100)
parser.add_argument('--window', type=int, default=5)
args = parser.parse_args()

node = memblast.start('slice_server', listen=args.listen, shape=[args.tickers, args.window])

index = 0
while True:
    with node.write() as arr:
        arr = arr.reshape(args.tickers, args.window)
        for i in range(args.tickers):
            # Slide existing values to the left and append a new one at the end
            arr[i, :-1] = arr[i, 1:]
            arr[i, -1] = random.uniform(100.0, 200.0)
        node.send_meta({'index': index})
    index += 1
    with node.read() as data:
        data = data.reshape(args.tickers, args.window)
        print("\033[H\033[J", end="")
        print(data)
        sys.stdout.flush()
    time.sleep(1)
