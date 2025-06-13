import argparse
import time
import memblast
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--server', default='0.0.0.0:7010')
parser.add_argument('--check-hash', action='store_true', help='verify snapshots')
args = parser.parse_args()

def handle_update(meta):
    print("metadata", meta)


node = memblast.start("b", server=args.server, shape=[10,10], on_update=handle_update,
                      check_hash=args.check_hash)

while True:
    with node.read() as arr:
        print("\033[H\033[J", end="")  # Move cursor to home position and clear screen
        print(f"version: {node.version}")
        print(arr)
        sys.stdout.flush()
    time.sleep(1)

