import argparse
import torch
import memblast
import sys

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

net = Net().float()
vec = torch.nn.utils.parameters_to_vector(net.parameters())

X = torch.tensor([[0.0, 0.0], [0.0, 1.0], [1.0, 0.0], [1.0, 1.0]])

def load_weights(buf):
    t = torch.frombuffer(buf, dtype=torch.float32)
    torch.nn.utils.vector_to_parameters(t, net.parameters())

async def handle_update(node, meta):
    with node.read() as arr:
        load_weights(memoryview(arr).cast("B"))
        out = net(X)
        preds = (out > 0.5).int().view(-1).tolist()
        print("\033[H\033[J", end="")
        for inp, p in zip(X.int().tolist(), preds):
            print(f"{inp} -> {p}")
        sys.stdout.flush()

memblast.start(
    "nn_client",
    server=args.server,
    shape=[len(vec)],
    on_update_async=handle_update,
)
