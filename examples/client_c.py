import argparse
import random
import time
import numpy as np
import raftmem

parser = argparse.ArgumentParser()
parser.add_argument('--listen', default='0.0.0.0:7012')
parser.add_argument('--peers', nargs='+', default=['0.0.0.0:7010', '0.0.0.0:7011'])
args = parser.parse_args()

node = raftmem.Node(args.listen, args.peers)
while True:
    with node.read() as arr:
        print(arr)  # will follow writes from other nodes
    time.sleep(1)