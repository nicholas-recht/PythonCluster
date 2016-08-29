# PythonCluster

This repository contains two simple python modules for distributing tasks among a cluster of nodes. The two modules are: dce.py and dce_node.py

dce.py contains the Cluster class, which handles the connection to, distribution of tasks among, a given cluster of nodes.

dce_node.py is the module run on each node to listen for jobs and execute them. 

## dce.py Usage
The only necessary import from dce.py is the Cluster class. Instances should be created with the "with Cluster(...) as ...:" construct so that necessary cleanup is always done. 

The Cluster constructor takes 3 parameters, a function "entry_point", a list of nodes, and an optional list of module dependencies. 

The "entry_point" function is the primary job to be executed on each node. It should be "stand-alone" in the sense that all dependencies are either included in the module dependency list, or imported within the function if the modules are available on each node. For example, suppose the "entry_point" was the following function:  

    def func(i):
    import time
    import random

    time.sleep(random.randint(0, 100) / 100.0)
    return i

Since time and random are both built-in modules, they can be imported within the "entry_point" function so they can be used.

The list of nodes take the form of a list of lists, where each inner-list has the IP address and port where a dce_node.py module is configured. A sample list, for example, could be:  

    (("192.168.1.24", 15809), ("192.168.1.25", 15809), ("192.168.1.26", 15819))  

The module dependencies is a list of module objects to be used as a dependency for each "entry_point" function. The source code for these must be locally available so that it can be sent to each node. 

For example, suppose another module "test_module.py", was created as part of a project. The Cluster initialization might look like the following:

    import test_module
    with Cluster(job=func, node_list=(("192.168.1.24", 15809), ("192.168.1.25", 15809), ("192.168.1.26", 15819)), module_dependencies=(test_module,)) as cluster:
      ...

test_module would then be available to be used within the given "entry_point" as if it were globally imported. 

### Multi-threading
The Cluster constructor contains an optional multi parameter (which by default is set to False) which can be used to execute multiple jobs per each node. By default, jobs will only be scheduled one at a time per node. With multi set to True, each node will be scheduled with as many jobs as available processors on the node. This can have a substantial performance increase when other jobs are not running on the nodes, and each job runs in a single thread. 

## dce_node.py Usage
dce_node.py is simply executed from the command-line, which starts an infinite process to wait for jobs from dce.py. dce_node.py can be given two parameters: the ip address and port to bind to. 

For example:

    python dce_node.py 192.168.1.24 15809

## Shared Clusters
All nodes which run dce_node.py can be shared by multiple programs running dce.py with the same set of nodes. Resources will be shared on the each node when executing multiple jobs at the same time, but everything else should function as expected. 

The other files in the repository are test files which demonstrate the basic usage and capabilities of the modules. 

## list_generator
A third module, list_generator.py, is also included in the repository. It contains a single helper function, from_range, which can be used to generate a list of node addresses given a start and end ip address. 
