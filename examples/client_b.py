import raftmem, numpy as np, random, time
node = raftmem.start(name="b", listen="0.0.0.0:7001", peers=["nodeA:7000"])
arr  = node.ndarray
while True:
    print("B", arr)     # will follow writes from A
    time.sleep(1)

