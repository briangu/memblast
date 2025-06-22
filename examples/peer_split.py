import argparse
import time
import memblast
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--name', choices=['a', 'b', 'c', 'd'], required=True,
                    help='peer identity (a-d)')
parser.add_argument('--listen')
parser.add_argument('--peers', nargs='*')
parser.add_argument('--size', type=int, default=8,
                    help='dimension of the square array (even)')
args = parser.parse_args()

dim = args.size
half = dim // 2

orders = {'a': 0, 'b': 1, 'c': 2, 'd': 3}
ports = {'a': 7100, 'b': 7101, 'c': 7102, 'd': 7103}

ordinal = orders[args.name]

listen = args.listen or f'0.0.0.0:{ports[args.name]}'
default_peers = [f'0.0.0.0:{ports[n]}' for n in orders if n != args.name]
servers = args.peers or default_peers

coords = {
    'a': [0, 0],
    'b': [0, half],
    'c': [half, 0],
    'd': [half, half],
}

start = coords[args.name]
maps = [ (c, [half, half], c, None) for p, c in coords.items() if p != args.name ]

node = memblast.start(
    f'peer_{args.name}',
    listen=listen,
    servers=servers,
    shape=[dim, dim],
    maps=maps,
)

index = 0
while True:
    with node.write() as arr:
        # Each peer writes a unique value based on its ordinal and the
        # current loop counter. Using modulus keeps the numbers small
        # so changes between updates are visible.
        val = float((index + ordinal) % 4)
        arr[start[0]:start[0]+half, start[1]:start[1]+half] = val
        node.version_meta({'index': index})
    with node.read() as arr:
        print("\033[H\033[J", end="")
        version_str = ' '.join(f'{k}={v}' for k, v in node.version.items())
        print(f'peer {args.name} versions: {version_str}')
        for r in range(dim):
            row_vals = arr[r]
            print(' '.join(f'{v:5.1f}' for v in row_vals))
        sys.stdout.flush()
    index += 1
    time.sleep(1)
