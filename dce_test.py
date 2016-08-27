from dce import Cluster
import sys
import time
import list_generator


def func(i):
    import time
    import random

    time.sleep(random.randint(0, 100) / 100.0)
    return i


def main(args):
    start_time = time.time()
    with Cluster(job=func, node_list=list_generator.from_range("192.168.1.16", "192.168.1.19", 15807)) as cluster:

        jobs = []

        for i in range(0, 100):
            jobs.append(cluster.execute(args=(i,)))

        for job in jobs:
            print(job())

    print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == "__main__":
    main(sys.argv)

