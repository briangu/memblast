import argparse
import time
import numpy as np
import duckdb
import raftmem

parser = argparse.ArgumentParser()
parser.add_argument('--peers', default='0.0.0.0:7010')
args = parser.parse_args()

node = raftmem.start("duck", server=args.peers, shape=[10])
con = duckdb.connect()

while True:
    with node.read() as arr:
        # make a copy so duckdb owns the buffer
        np_arr = np.array(arr)
        con.register('raft', np_arr)
        result = con.execute('SELECT AVG(column0) FROM raft').fetchall()[0][0]
        print("\033[H\033[J", end="")
        print('average', result)
    time.sleep(1)

