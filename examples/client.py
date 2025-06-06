import argparse
import random
import time
import numpy as np
import raftmem

parser = argparse.ArgumentParser()
parser.add_argument('--server', default='0.0.0.0:7010')
args = parser.parse_args()

def on_update(data):
    print(f"Update {data}")

node = raftmem.start("b", server=args.server, shape=[100], on_update=on_update)

while True:
    with node.read() as arr:
        print(arr)  # will follow writes from A
    time.sleep(1)
