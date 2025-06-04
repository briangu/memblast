import argparse
import random
import time
import numpy as np
import raftmem

parser = argparse.ArgumentParser()
parser.add_argument('--listen', default='0.0.0.0:7010')
parser.add_argument('--peers', nargs='+', default=['0.0.0.0:7011', '0.0.0.0:7012'])
args = parser.parse_args()

def on_leader():
    print("A became leader")

def on_update():
    print("A updated")

TICKERS = ["AAPL", "GOOG", "MSFT"]
SECONDS_PER_DAY = 2340
TOTAL = len(TICKERS) * SECONDS_PER_DAY

node = raftmem.start(
    "a",
    args.listen,
    args.peers,
    shape=[TOTAL],
    on_leader=on_leader,
    on_update=on_update,
)

position = 0
high_water = 0

while True:
    if node.leader and position < TOTAL:
        with node.write() as w:
            arr = w.ndarray
            for _ in range(len(TICKERS)):
                if position >= TOTAL:
                    break
                arr[position] = random.random() * 100
                w.dirty(position)
                position += 1
            high_water = position
        print("high water", high_water)
    with node.read() as arr:
        print(arr[:high_water])
    time.sleep(1)

