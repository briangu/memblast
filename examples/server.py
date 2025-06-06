import argparse
import random
import time
import numpy as np
import raftmem

parser = argparse.ArgumentParser()
parser.add_argument('--listen', default='0.0.0.0:7010')
args = parser.parse_args()

node = raftmem.start("a", listen=args.listen, shape=[10,10])

while True:
    with node.write() as a:
        for _ in range(3):
            idx = random.randrange(len(a))
            a[idx] = random.random()
    with node.read() as arr:
        print(arr)
    time.sleep(1)                    # write flushes on __exit__

