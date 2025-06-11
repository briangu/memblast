import argparse
import time
import numpy as np
import torch
import memblast
import sys
from contextlib import contextmanager

parser = argparse.ArgumentParser()
parser.add_argument('--server', default='0.0.0.0:7040')
args = parser.parse_args()

class Net(torch.nn.Module):
    def __init__(self):
        super().__init__()
        self.l1 = torch.nn.Linear(2, 2)
        self.l2 = torch.nn.Linear(2, 1)

    def forward(self, x):
        x = torch.tanh(self.l1(x))
        return torch.sigmoid(self.l2(x))

net = Net().double()
vec = torch.nn.utils.parameters_to_vector(net.parameters())
node = memblast.start("nn_client", server=args.server, shape=[len(vec)])

@contextmanager
def read_bytes(node):
    with node.read() as arr:
        yield memoryview(arr).cast("B")

X = torch.tensor([[0.,0.],[0.,1.],[1.,0.],[1.,1.]])

def load_weights(buf):
    t = torch.frombuffer(buf, dtype=torch.float64)
    torch.nn.utils.vector_to_parameters(t, net.parameters())

while True:
    with read_bytes(node) as buf:
        load_weights(buf)
        out = net(X)
        preds = (out > 0.5).int().view(-1).tolist()
        print("\033[H\033[J", end="")
        for inp, p in zip(X.int().tolist(), preds):
            print(f"{inp} -> {p}")
        sys.stdout.flush()
    time.sleep(1)
