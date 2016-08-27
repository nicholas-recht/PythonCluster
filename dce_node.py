import marshal
import types
import Pyro4
import sys
import base64
import multiprocessing


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

    def _get_caller_address(self):
        address = Pyro4.current_context.client.sock.getpeername()[0] + ":" \
                  + str(Pyro4.current_context.client.sock.getpeername()[1])

        return address

    def add_module(self, name, source):
        """
        Add the module as a dependency for the caller
        :param name:
        :param source:
        :return:
        """
        address = self._get_caller_address()

        mod = create_new_module(name, source)

        if address not in self.contexts:
            self.contexts[address] = {}
        self.contexts[address][name] = mod

    def get_num_processors(self):
        """
        Returns the number of processors available to the node
        :return:
        """
        return self._num_processors

    def set_entry_point(self, params):
        """
        Sets the entry point function to use for the caller
        :param params:
        :return:
        """
        address = self._get_caller_address()

        code_string = base64.b64decode(params["data"])
        # create the context for the entry point
        context = globals()
        # if any modules were added, then merge them into the context
        if address in self.contexts:
            context.update(self.contexts[address])

        self.entry_points[address] = deserialize_function(code_string, context)

    def execute(self, args=(), kwargs=None):
        """
        Executes the given entry_point function for the caller using the given arguments
        and returns the result
        :param args:
        :param kwargs:
        :return:
        """
        if kwargs is None: kwargs = {}

        address = self._get_caller_address()

        return self.entry_points[address](*args, **kwargs)


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
    with Pyro4.Daemon(port=port, host=host) as daemon:
        uri = daemon.register(node, "dce_node")
        print("Daemon started")
        daemon.requestLoop()

if __name__ == "__main__":
    main(sys.argv)
