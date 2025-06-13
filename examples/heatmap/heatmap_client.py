import argparse
import time
import numpy as np
import memblast
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--server', default='0.0.0.0:7050')
parser.add_argument('--size', type=int, default=128)
args = parser.parse_args()

size = args.size
node = memblast.start('heatmap_client', server=args.server, shape=[size, size])
chars = " .:-=+*#%@"

while True:
    with node.read() as arr:
        arr = arr.reshape(size, size)
        norm = (arr - arr.min()) / (arr.ptp() + 1e-9)
        levels = (norm * (len(chars) - 1)).astype(int)
        print("\033[H\033[J", end="")
        for row in levels:
            print(''.join(chars[i] for i in row))
        sys.stdout.flush()
    time.sleep(0.1)
