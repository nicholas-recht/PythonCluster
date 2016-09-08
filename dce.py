import marshal
import Pyro4
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from threading import Event
import inspect


def _serialize_function(func):
    code_string = marshal.dumps(func.__code__)

    return code_string


def _get_module_name(mod):
    return inspect.getmodule(mod).__name__


def _get_module_source(mod):
    return inspect.getsource(mod)


class PendingFuture:
    """
    Abstraction for getting the result from a job
    either returns the future object's result method
    or blocks until a future has been added when the
    job is waiting for a node to become available
    """
    def __init__(self):
        self._future = None
        self._event = Event()

    def __call__(self, *args, **kwargs):
        # wait until a future object was created
        self._event.wait()
        return self._future.result()

    def add_future(self, future):
        self._future = future
        self._event.set()


class JobDoneCallable:
    """
    Callable for when a node has finished it's current job.
    stores the node which was used to execute the job
    so it can add that node back to the queue when finished then
    return control back to the Cluster
    """
    def __init__(self, node, callback):
        self.node = node
        self.callback = callback

    def __call__(self, *args, **kwargs):
        self.callback(self.node)


class Node:
    """
    Each node used for executing jobs. Stores the associated id value to use, the Pyro4
    proxy object, and the number of processors available (if the node is the original)
    """
    def __init__(self, address, port):
        self.daemon = Pyro4.Proxy("PYRO:dce_node@" + address + ":" + str(port))
        self.id = -1
        self.procs = 1


class Cluster:
    """
    The main class used for distributing jobs among a list of nodes
    """
    def __init__(self, job, node_list, module_dependencies=(), multi=False):
        """
        Creates a new cluster and sets up the cluster to schedule new instances of the given
        job.
        :param job:
        :param node_list:
        :param module_dependencies:
        :param multi:
        """
        self._nodes = []
        self._futures = []
        self._pending_jobs = Queue()
        self._node_queue = Queue()
        self._id = -1
        self._module_d = module_dependencies
        self._multi = multi

        # set up the nodes to use
        for address in node_list:
            node = Node(address[0], address[1])
            self._nodes.append(node)

        # get the id values to use for each
        self._get_id_values()

        # send any module dependencies
        self._send_modules()

        # send each node the entry point to use
        code_string = _serialize_function(job)
        self._setup(code_string)

        # set up multi-processing if set
        if self._multi:
            self._set_multi_processing()
            # if multi threaded, then add more copies of each node for the number of threads available
            for i in range(len(node_list)):
                address = node_list[i]
                original_node = self._nodes[i]
                for j in range(original_node.procs - 1):
                    node = Node(address[0], address[1])
                    node.id = original_node.id  # copy the id value from the copy node
                    self._nodes.append(node)

        # add each node to the queue
        for node in self._nodes:
            self._node_queue.put(node)

        # create the thread pool which will now be used to schedule jobs
        self.dispatcher = ThreadPoolExecutor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.exit()

    def _get_id_values(self):
        results = []
        with ThreadPoolExecutor() as pool:
            # get the id value of each node to use
            for node in self._nodes:
                results.append(pool.submit(node.daemon.new_id))

            for i in range(len(results)):
                self._nodes[i].id = results[i].result()

    def _set_multi_processing(self):
        results = []
        with ThreadPoolExecutor() as pool:
            # get the number of processors in each node
            for node in self._nodes:
                results.append(pool.submit(node.daemon.get_num_processors))

            for i in range(len(results)):
                self._nodes[i].procs = results[i].result()

    def _send_modules(self):
        with ThreadPoolExecutor() as pool:
            # send the module dependency to each node
            for mod in self._module_d:
                name = _get_module_name(mod)
                source = _get_module_source(mod)

                for node in self._nodes:
                    pool.submit(node.daemon.add_module, node.id, name, source)

    def _setup(self, func):
        with ThreadPoolExecutor() as pool:
            # send the given entry point function to each node
            for node in self._nodes:
                pool.submit(node.daemon.set_entry_point, node.id, func)

    def _add_node_to_queue(self, node):
        self._node_queue.put(node)
        self._check_for_jobs()

    def _check_for_jobs(self):
        if not self._pending_jobs.empty() and not self._node_queue.empty():
            params = self._pending_jobs.get()
            node = self._node_queue.get()

            pf = params[0]
            args = params[1]
            kwargs = params[2]

            self._execute(pf, node, args, kwargs)

    def _execute(self, pending_future, node, args, kwargs):
        fut = self.dispatcher.submit(node.daemon.execute, node.id, args=args, kwargs=kwargs)

        # create the callback function to use
        cal = JobDoneCallable(node, self._add_node_to_queue)

        fut.add_done_callback(cal)
        pending_future.add_future(fut)

    def execute(self, args=(), kwargs=None):
        """
        Executes a new job using the given arguments and returns a callable object which
        returns the result of the job when ready
        :param args:
        :param kwargs:
        :return:
        """
        if kwargs is None: kwargs = {}

        # create the callable
        pf = PendingFuture()

        # add the "job" to the pending_jobs
        args = (pf, args, kwargs)
        self._pending_jobs.put(args)

        # call check_for_jobs
        self._check_for_jobs()
        self._futures.append(pf)

        # return the callable
        return pf

    def exit(self):
        """
        Shuts down this Cluster instance and signals all connected nodes that this
        entry point is no longer being used
        :return:
        """
        for node in self._nodes:
            self.dispatcher.submit(node.daemon.end_id, node.id)
        self.dispatcher.shutdown(True)


