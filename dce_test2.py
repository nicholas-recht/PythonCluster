from dce import Cluster
import sys
import time


def func(i):
    import time
    import random

    time.sleep(random.randint(0, 100) / 100.0)
    return i


def main(args):
    start_time = time.time()
    with Cluster(job=func, node_list=(
            ("192.168.1.16", 15807),
            ("192.168.1.17", 15807),
            ("192.168.1.18", 15807),
            ("192.168.1.19", 15807)
    ), multi=True) as cluster:

        jobs = []

        for i in range(0, 200):
            jobs.append(cluster.execute(args=(i,)))

        for job in jobs:
            print(job())

    print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == "__main__":
    main(sys.argv)

