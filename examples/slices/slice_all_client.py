import argparse
import memblast
import sys


async def handle_update(node, meta):
    print("metadata", meta)
    with node.read() as arr:
        print("\033[H\033[J", end="")
        print(arr)
        sys.stdout.flush()

parser = argparse.ArgumentParser()
parser.add_argument('--server', default='0.0.0.0:7020')
args = parser.parse_args()

node = memblast.start(
    "b",
    server=args.server,
    shape=[100, 5],
    on_update_async=handle_update,
)

