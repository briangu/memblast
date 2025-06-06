import argparse
import random
import time
import numpy as np
import raftmem

# ticker plant server
# rust library listens for clients and updates shared memory on the listener
#

parser = argparse.ArgumentParser()
parser.add_argument('--listen', default='0.0.0.0:7010')
args = parser.parse_args()

node = raftmem.start("a", listen=args.listen, shape=[100])

idx = 0
while True:
    with node.write() as a:
        a[idx] = random.random()
        a.update({"position": idx})
    idx = (idx + 1) % len(a)
    with node.read() as arr:
        print(arr)
    time.sleep(1)                    # write flushes on __exit__

