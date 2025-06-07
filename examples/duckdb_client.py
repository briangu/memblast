import argparse
import time
import numpy as np
import duckdb
import memblast

parser = argparse.ArgumentParser()
parser.add_argument('--peers', default='0.0.0.0:7010')
args = parser.parse_args()

node = memblast.start("duck", server=args.peers, shape=[10,10])
con = duckdb.connect()

# Register the numpy array ONCE. This is a live view into the shared memory,
# so DuckDB will always see the latest data without re-registering.
arr = node.ndarray()
arr = arr.reshape([10,10])
con.register('data', arr)

while True:
    with node.read() as arr:
        result = con.execute('SELECT AVG(column0) FROM data').fetchall()[0][0]
        print("\033[H\033[J", end="")
        print('average', result)
    time.sleep(1)

