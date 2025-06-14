import argparse
import time
import memblast
import duckdb
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument('--server', default='0.0.0.0:7013')
parser.add_argument('--snap1', type=int, default=1, help='First snapshot version')
parser.add_argument('--snap2', type=int, default=2, help='Second snapshot version')
args = parser.parse_args()

node = memblast.start('snapshot_duck_client', server=args.server, shape=[5], snapshots=True)

con = duckdb.connect()

# Wait for the snapshots to exist on the server
print('waiting for snapshots...')
time.sleep(2)

snap1 = node.version_data(args.snap1)
snap2 = node.version_data(args.snap2)

if snap1 is None or snap2 is None:
    print('Snapshots not available.')
    exit(1)

arr1 = np.array(snap1)
arr2 = np.array(snap2)
con.register('s1', arr1)
con.register('s2', arr2)

m1 = con.execute('SELECT AVG(*) FROM s1').fetchall()[0][0]
m2 = con.execute('SELECT AVG(*) FROM s2').fetchall()[0][0]

print(f'Snapshot {args.snap1} mean: {m1:.4f}')
print(f'Snapshot {args.snap2} mean: {m2:.4f}')
