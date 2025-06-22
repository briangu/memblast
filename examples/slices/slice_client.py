import argparse
import memblast
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--server', default='0.0.0.0:7020')
parser.add_argument('--tickers', default='2,50,75')
parser.add_argument('--window', type=int, default=5)
args = parser.parse_args()

tickers = [int(t) for t in args.tickers.split(',') if t]

maps = []
for i, t in enumerate(tickers):
    maps.append(([t, 0], [1, args.window], [i, 0], None))

window = args.window

async def handle_update(node, meta):
    with node.read() as arr:
        print("\033[H\033[J", end="")
        for t, row in zip(tickers, arr):
            print(f'{t}: {row}')
        sys.stdout.flush()

memblast.start(
    'slice_client',
    server=args.server,
    shape=[len(tickers), window],
    maps=maps,
    on_update_async=handle_update,
)
