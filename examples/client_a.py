import raftmem, numpy as np, random, time
node = raftmem.start(name="a", listen="0.0.0.0:7000", peers=["nodeB:7001"])
arr  = node.ndarray     # shared   [f64;10]
while True:
    idx = random.randrange(10); val = random.random()*10
    arr[idx] = val       # ← real NumPy write, replicated
    node.flush(idx)      # tell raftmem to broadcast dirty slot
    print("A", arr)
    time.sleep(1)
