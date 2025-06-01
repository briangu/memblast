import raftmem, numpy as np, random, time
node = raftmem.start(listen="0.0.0.0:7011", peers=["0.0.0.0:7010"])
while True:
    with node.read() as arr:
        print(arr)  # will follow writes from A
    time.sleep(1)

