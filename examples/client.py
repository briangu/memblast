import argparse
import memblast
import sys


async def handle_update(node, meta):
    print("metadata", meta)
    with node.read() as arr:
        print("\033[H\033[J", end="")
        remote = node.version.get(args.server, 0)
        print(f"version: {remote}")
        print(arr)
        sys.stdout.flush()

parser = argparse.ArgumentParser()
parser.add_argument('--server', default='0.0.0.0:7010')
parser.add_argument('--check-hash', action='store_true', help='verify snapshots')
args = parser.parse_args()

async def on_connect(node, sub):
    print('connected to', sub['name'])


async def on_disconnect(node, peer):
    print('disconnected from', peer)

memblast.start(
    "b",
    server=args.server,
    shape=[10, 10],
    on_update_async=handle_update,
    on_connect=on_connect,
    on_disconnect=on_disconnect,
    check_hash=args.check_hash,
)

