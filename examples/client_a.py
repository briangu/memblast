import raftmem, random, time, numpy as np

node = raftmem.start("0.0.0.0:7010", ["0.0.0.0:7011"])
a = node.ndarray

while True:
    with node.batch():               # writes accumulate locally
        for _ in range(8):
            a[random.randrange(10)] = random.random() * 10
    print(a)
    time.sleep(1)                  # batch flushes on __exit__

