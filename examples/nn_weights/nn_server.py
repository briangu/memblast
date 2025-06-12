import argparse
import time
import numpy as np
import torch
import memblast
from contextlib import contextmanager

parser = argparse.ArgumentParser()
parser.add_argument('--listen', default='0.0.0.0:7040')
args = parser.parse_args()

class Net(torch.nn.Module):
    def __init__(self):
        super().__init__()
        self.l1 = torch.nn.Linear(2, 2)
        self.l2 = torch.nn.Linear(2, 1)

    def forward(self, x):
        x = torch.tanh(self.l1(x))
        return torch.sigmoid(self.l2(x))

net = Net().float()
vec = torch.nn.utils.parameters_to_vector(net.parameters())
node = memblast.start("nn_server", listen=args.listen, shape=[len(vec)])

@contextmanager
def write_bytes(node):
    with node.write() as arr:
        yield memoryview(arr).cast("B")

X = torch.tensor([[0.,0.],[0.,1.],[1.,0.],[1.,1.]])
Y = torch.tensor([[0.],[1.],[1.],[0.]])

optim = torch.optim.SGD(net.parameters(), lr=0.1)
loss_fn = torch.nn.BCELoss()

while True:
    optim.zero_grad()
    out = net(X)
    loss = loss_fn(out, Y)
    loss.backward()
    optim.step()
    vec = torch.nn.utils.parameters_to_vector(net.parameters())
    with write_bytes(node) as buf:
        t = torch.frombuffer(buf, dtype=torch.float64)
        t.copy_(vec)
    time.sleep(1)
