import raftmem, random, time, numpy as np

node = raftmem.start("0.0.0.0:7010", ["0.0.0.0:7011"])

while True:
    with node.write() as a:          # writes accumulate locally
        for _ in range(8):
            a[random.randrange(10)] = random.random() * 10
    print(a)
    time.sleep(1)                    # write flushes on __exit__

