import marshal
import types
import Pyro4
import sys
import base64
import multiprocessing
import queue


def deserialize_function(code_string, context):
    code = marshal.loads(code_string)
    func = types.FunctionType(code, context, "entry_point")
    return func


def create_new_module(name, code):
    # if name not in sys.modules:
        # create the module
        mod = types.ModuleType(name)
        # execute the module's code
        exec(code, mod.__dict__)

        return mod


@Pyro4.expose
class Node:
    def __init__(self):
        self.entry_points = {}
        self.contexts = {}
        self._num_processors = multiprocessing.cpu_count()
        self._i_id = 0
        self._i_queue = queue.Queue()

    def _get_caller_address(self):
        address = Pyro4.current_context.client.sock.getpeername()[0] + ":" \
                  + str(Pyro4.current_context.client.sock.getpeername()[1])

        return address

    def new_id(self):
        """
        Returns the id value which can be used to associate the given caller with its
        entry point and module dependencies
        :return:
        """
        if self._i_queue.empty():
            num = self._i_id

            self._i_id += 1

            return num
        else:
            return self._i_queue.get()

    def add_module(self, id, name, source):
        """
        Add the module as a dependency for the caller
        :param id:
        :param name:
        :param source:
        :return:
        """
        mod = create_new_module(name, source)

        if id not in self.contexts:
            self.contexts[id] = {}
        self.contexts[id][name] = mod

    def get_num_processors(self):
        """
        Returns the number of processors available to the node
        :return:
        """
        return self._num_processors

    def set_entry_point(self, id, params):
        """
        Sets the entry point function to use for the caller
        :param id:
        :param params:
        :return:
        """
        code_string = base64.b64decode(params["data"])
        # create the context for the entry point
        context = globals()
        # if any modules were added, then merge them into the context
        if id in self.contexts:
            context.update(self.contexts[id])

        self.entry_points[id] = deserialize_function(code_string, context)

    def execute(self, id, args=(), kwargs=None):
        """
        Executes the given entry_point function for the caller using the given arguments
        and returns the result
        :param args:
        :param kwargs:
        :return:
        """
        if kwargs is None: kwargs = {}

        return self.entry_points[id](*args, **kwargs)

    def end_id(self, id):
        """
        Releases the entry point and any modules specified by the given caller using
        its asigned id value
        :param id:
        :return:
        """
        self._i_queue.put(id)

        self.entry_points.pop(id)
        self.contexts.pop(id)


def main(args):
    """
    Starts up a daemon process on the given address and port to listen for job requests
    :param args:
    :return:
    """
    node = Node()

    host = "localhost"
    port = 17888

    if len(args) > 1:
        host = args[1]
    if len(args) > 2:
        port = int(args[2])

    print("Start daemon")

    # NAT
    if len(args) > 4:
        nat_host = args[3]
        nat_port = int(args[4])

        with Pyro4.Daemon(natport=nat_port, nathost=nat_host, port=port, host=host) as daemon:
            uri = daemon.register(node, "dce_node")
            print("Daemon started")
            daemon.requestLoop()

    # no NAT
    else:
        with Pyro4.Daemon(port=port, host=host) as daemon:
            uri = daemon.register(node, "dce_node")
            print("Daemon started")
            daemon.requestLoop()

if __name__ == "__main__":
    main(sys.argv)
