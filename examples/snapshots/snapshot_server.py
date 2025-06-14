import argparse
import time
import numpy as np
import memblast
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--listen', default='0.0.0.0:7013')
parser.add_argument('--size', type=int, default=5)
args = parser.parse_args()

node = memblast.start('snapshot_server', listen=args.listen, shape=[args.size], snapshots=True)

while True:
    with node.write() as arr:
        arr[:] = np.random.random(args.size)
    node.take_snapshot()
    with node.read() as arr:
        print("\033[H\033[J", end="")
        print(f'version: {node.version}')
        print(arr)
        sys.stdout.flush()
    time.sleep(1)
