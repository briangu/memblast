import argparse
import random
import time
import numpy as np
import raftmem

parser = argparse.ArgumentParser()
parser.add_argument('--listen', default='0.0.0.0:7012')
parser.add_argument('--peers', nargs='+', default=['0.0.0.0:7010', '0.0.0.0:7011'])
args = parser.parse_args()

node = raftmem.start("c", args.listen, args.peers)
arr = node.ndarray

while True:
    print(arr)  # will follow writes from other nodes
    time.sleep(1)
