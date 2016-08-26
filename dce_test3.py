from dce import Cluster
import sys
import time
import test_dependency


def func(i):
    return test_dependency.func(i)


def main(args):
    start_time = time.time()
    with Cluster(job=func, node_list=(("192.168.1.16", 15807), ("192.168.1.17", 15807),
                                      ("192.168.1.18", 15807), ("192.168.1.19", 15807)),
                 module_dependencies=(test_dependency,)) as cluster:

        jobs = []

        for i in range(0, 100):
            jobs.append(cluster.execute(args=(i,)))

        for job in jobs:
            print(job())

    print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == "__main__":
    main(sys.argv)

