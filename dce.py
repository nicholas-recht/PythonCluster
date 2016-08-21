import marshal
import Pyro4
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from threading import Event


def _serialize_function(func):
    code_string = marshal.dumps(func.__code__)

    return code_string


# Abstraction for getting the result from a job
#   either returns the future object's result method
#   or blocks until a future has been added when the
#   job is waiting for a node to become available
class PendingFuture:
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


# Callable for when a node has finished it's current job
#   stores the node which was used to execute the job
#   so it can add that node back to the queue when finished then
#   return control back to the Cluster
class JobDoneCallable:
    def __init__(self, node, callback):
        self.node = node
        self.callback = callback

    def __call__(self, *args, **kwargs):
        self.callback(self.node)


class Cluster:
    def __init__(self, job, node_list):
        self._nodes = []
        self._futures = []
        self._pending_jobs = Queue()
        self._node_queue = Queue()
        self._id = -1

        # set up the nodes to use
        for address in node_list:
            self._nodes.append(Pyro4.Proxy("PYRO:dce_node@" + address[0] + ":" + str(address[1])))

        code_string = _serialize_function(job)

        # send each node the entry point to use
        # for node in self._nodes:
        #     node.set_entry_point(code_string)
        self._setup(code_string)

        # add each node to the queue
        for node in self._nodes:
            self._node_queue.put(node)

        # create the thread pool which will now be used to schedule jobs
        self.dispatcher = ThreadPoolExecutor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dispatcher.shutdown(True)

    def _setup(self, func):
        with ThreadPoolExecutor() as pool:
            # send the given entry point function to each node
            for node in self._nodes:
                pool.submit(node.set_entry_point, func)

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
        fut = self.dispatcher.submit(node.execute, args=args, kwargs=kwargs)

        # create the callback function to use
        cal = JobDoneCallable(node, self._add_node_to_queue)

        fut.add_done_callback(cal)
        pending_future.add_future(fut)

    def execute(self, args=(), kwargs=None):
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


