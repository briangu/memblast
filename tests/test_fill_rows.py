import memblast

def test_fill_rows():
    node = memblast.start("rows", shape=[10, 10])
    with node.write() as arr:
        for i in range(10):
            arr[i] = float(i)
    with node.read() as arr:
        for i in range(10):
            for j in range(10):
                assert arr[i, j] == float(i)
