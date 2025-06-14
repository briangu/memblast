import argparse
import time
import memblast
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--name', choices=['a', 'b'], required=True,
                    help='peer identity (a or b)')
parser.add_argument('--listen')
parser.add_argument('--peer')
parser.add_argument('--size', type=int, default=10)
args = parser.parse_args()

size = args.size
half = size // 2

if args.name == 'a':
    listen = args.listen or '0.0.0.0:7100'
    peer = args.peer or '0.0.0.0:7101'
    local_start = 0
    remote_start = half
else:
    listen = args.listen or '0.0.0.0:7101'
    peer = args.peer or '0.0.0.0:7100'
    local_start = half
    remote_start = 0

maps = [([remote_start], [half], [remote_start], None)]

node = memblast.start(
    f'peer_{args.name}',
    listen=listen,
    shape=[size],
)
node.connect(peer, maps=maps)

index = 0
while True:
    with node.write() as arr:
        arr[local_start + index % half] = float(index)
        node.send_meta({'index': index})
    with node.read() as arr:
        print("\033[H\033[J", end="")
        print(f'peer {args.name} version: {node.version}')
        print(list(arr))
        sys.stdout.flush()
    index += 1
    time.sleep(1)
