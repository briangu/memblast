import argparse
import time
import numpy as np
import memblast
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--listen', default='0.0.0.0:7030')
args = parser.parse_args()

size = 256
node = memblast.start('life_server', listen=args.listen, shape=[size, size])

world = np.random.randint(2, size=(size, size)).astype(float)

# offsets for the eight neighbours
neigh = [(i, j) for i in (-1, 0, 1) for j in (-1, 0, 1) if not (i == 0 and j == 0)]

while True:
    # compute neighbour count using numpy roll
    neighbours = sum(np.roll(np.roll(world, i, axis=0), j, axis=1) for i, j in neigh)
    world = ((neighbours == 3) | ((world > 0) & (neighbours == 2))).astype(float)

    with node.write() as arr:
        arr = arr.reshape(size, size)
        arr[:, :] = world
    
    with node.read() as arr:
        arr = arr.reshape(size, size)
        print("\033[H\033[J", end="")
        display = np.where(arr > 0, '#', '.')
        for row in display:
            print(''.join(row))
        sys.stdout.flush()
    time.sleep(0.5)
