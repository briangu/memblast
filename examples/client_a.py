import argparse
import random
import time
import numpy as np
import raftmem

parser = argparse.ArgumentParser()
parser.add_argument('--listen', default='0.0.0.0:7010')
parser.add_argument('--peers', nargs='+', default=['0.0.0.0:7011', '0.0.0.0:7012'])
args = parser.parse_args()

node = raftmem.Node(args.listen, args.peers)

while True:
    with node.write() as a:          # writes accumulate locally
        for _ in range(8):
            a[random.randrange(10)] = random.random() * 10
    print(a)
    time.sleep(1)                    # write flushes on __exit__

