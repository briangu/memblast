import argparse
import random
import time
import numpy as np
import raftmem

parser = argparse.ArgumentParser()
parser.add_argument('--listen', default='0.0.0.0:7010')
parser.add_argument('--peers', nargs='+', default=['0.0.0.0:7011', '0.0.0.0:7012'])
args = parser.parse_args()

node = raftmem.start("a", args.listen, args.peers)

while True:
    with node.write() as a:
        for _ in range(8):
            idx = random.randrange(10)
            a[idx] = random.random() * 10
    with node.read() as arr:
        print(arr)
    time.sleep(1)                    # write flushes on __exit__

