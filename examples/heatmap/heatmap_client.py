import argparse
import numpy as np
import memblast
import sys


parser = argparse.ArgumentParser()
parser.add_argument('--server', default='0.0.0.0:7050')
parser.add_argument('--size', type=int, default=128)
args = parser.parse_args()

size = args.size
chars = " .:-=+*#%@"

async def handle_update(node, meta):
    global size, chars
    print(meta)
    with node.read() as arr:
        norm = (arr - arr.min()) / (np.ptp(arr) + 1e-9)
        levels = (norm * (len(chars) - 1)).astype(int)
        print("\033[H\033[J", end="")
        for row in levels:
            print(''.join(chars[i] for i in row))
        sys.stdout.flush()


memblast.start('heatmap_client', server=args.server, shape=[size, size], on_update_async=handle_update)

