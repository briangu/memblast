import argparse
import time
import raftmem

parser = argparse.ArgumentParser()
parser.add_argument('--peers', default='0.0.0.0:7010')
args = parser.parse_args()

node = raftmem.start("b", server=args.peers, shape=[10,10])

while True:
    with node.read() as arr:
        print(arr)  # will follow writes from A
    time.sleep(1)

