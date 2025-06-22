import argparse
import time
import numpy as np
import memblast
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--server', default='0.0.0.0:7030')
parser.add_argument('--width', type=int, default=64)
parser.add_argument('--region') # r0,c0,h,w (e.g. 0,0,10,10 for top left corner
args = parser.parse_args()

size = args.width

if args.region:
    r0, c0, h, w = [int(x) for x in args.region.split(',')]
    maps = [([r0, c0], [h, w], [0, 0], None)]
    node = memblast.start('life_client_region', server=args.server, shape=[h, w], maps=maps)
    view_shape = (h, w)
else:
    node = memblast.start('life_client', server=args.server, shape=[size, size])
    view_shape = (size, size)

while True:
    with node.read() as arr:
        print("\033[H\033[J", end="")
        display = np.where(arr > 0, '#', '.')
        for row in display:
            print(''.join(row))
        sys.stdout.flush()
    time.sleep(0.5)
