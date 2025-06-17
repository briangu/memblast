import argparse
import random
import time
import numpy as np
import memblast
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--listen', default='0.0.0.0:7010')
args = parser.parse_args()

node = memblast.start("a", listen=args.listen, shape=[10,10])

while True:
    with node.write() as a:
        last = 0
        for _ in range(3):
            idx = random.randrange(len(a))
            a[idx] = random.random()
            last = idx
        node.version_meta({"last_index": last})
    with node.read() as arr:
        print("\033[H\033[J", end="")
        print(f"version: {node.version.get(node.name, 0)}")
        print(arr)
        sys.stdout.flush()
    time.sleep(1)
