from dce import Cluster
import sys
import time
import test_dependency


def func(i):
    return test_dependency.func(i)


def main(args):
    start_time = time.time()
    with Cluster(job=func, node_list=(("localhost", 17888),), module_dependencies=(test_dependency,)) as cluster:

        jobs = []

        for i in range(0, 100):
            jobs.append(cluster.execute(args=(i,)))

        for job in jobs:
            print(job())

    print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == "__main__":
    main(sys.argv)

