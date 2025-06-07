import argparse
import time
import memblast
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--server', default='0.0.0.0:7010')
args = parser.parse_args()

node = memblast.start("b", server=args.server, shape=[10,10])


def handle_update(meta):
    print("metadata", meta)


node.on_update(handle_update)

while True:
    with node.read() as arr:
        print("\033[H\033[J", end="")  # Move cursor to home position and clear screen
        print(arr)
        sys.stdout.flush()
    time.sleep(1)

