import argparse
import time
import raftmem
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--peers', default='0.0.0.0:7020')
args = parser.parse_args()

node = raftmem.start("b", server=args.peers, shape=[100,5])


def handle_update(meta):
    print("metadata", meta)


node.on_update(handle_update)

while True:
    with node.read() as arr:
        print("\033[H\033[J", end="")  # Move cursor to home position and clear screen
        print(arr)
        sys.stdout.flush()
    time.sleep(1)

