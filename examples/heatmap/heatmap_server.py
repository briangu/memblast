import argparse
import time
import numpy as np
import memblast
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--listen', default='0.0.0.0:7050')
parser.add_argument('--size', type=int, default=128)
args = parser.parse_args()

size = args.size
x = np.linspace(0, np.pi * 4, size)
y = np.linspace(0, np.pi * 4, size)
X, Y = np.meshgrid(x, y)

node = memblast.start('heatmap_server', listen=args.listen, shape=[size, size])
phase = 0.0

while True:
    data = np.sin(X + phase) * np.cos(Y + phase)
    with node.write() as arr:
        arr = arr.reshape(size, size)
        arr[:] = data
        node.version_meta({'phase': float(phase)})
    with node.read() as arr:
        arr = arr.reshape(size, size)
        print("\033[H\033[J", end="")
        # display simple summary to avoid overwhelming the terminal
        print(f"phase: {phase:.2f} min {arr.min():.2f} max {arr.max():.2f}")
        sys.stdout.flush()
    phase += 0.1
    time.sleep(0.1)
