import argparse
import random
import time
import numpy as np
import raftmem

parser = argparse.ArgumentParser()
parser.add_argument('--listen', default='0.0.0.0:7012')
parser.add_argument('--peers', nargs='+', default=['0.0.0.0:7010', '0.0.0.0:7011'])
args = parser.parse_args()

def on_leader():
    print("C became leader")

def on_update():
    print("C got update")

node = raftmem.start("c", args.listen, args.peers, on_leader=on_leader, on_update=on_update)

while True:
    with node.read() as arr:
        print(arr)
    print("leader?", node.leader)
    time.sleep(1)
