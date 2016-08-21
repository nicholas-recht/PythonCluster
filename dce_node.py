import marshal
import types
import Pyro4
import sys
import base64
import queue


def deserialize_function(code_string):
    code = marshal.loads(code_string)
    func = types.FunctionType(code, globals(), "entry_point")
    return func


def create_new_module(name, code):
    # create the module
    mod = types.ModuleType(name)
    # execute the module's code
    exec(code) in mod.__dict__
    # add the module to the programs modules
    sys.modules[name] = mod


@Pyro4.expose
class Node:
    def __init__(self):
        self.entry_points = {}

    def _get_caller_address(self):
        address = Pyro4.current_context.client.sock.getpeername()[0] + ":" \
                  + str(Pyro4.current_context.client.sock.getpeername()[1])

        return address

    def set_entry_point(self, params):
        address = self._get_caller_address()

        code_string = base64.b64decode(params["data"])
        self.entry_points[address] = deserialize_function(code_string)

    def execute(self, args=(), kwargs=None):
        if kwargs is None: kwargs = {}

        address = self._get_caller_address()

        return self.entry_points[address](*args, **kwargs)


def main(args):
    node = Node()

    print("Start daemon")
    with Pyro4.Daemon(port=17888, host="localhost") as daemon:
        uri = daemon.register(node, "dce_node")
        print("Daemon started")
        daemon.requestLoop()

if __name__ == "__main__":
    main(sys.argv)
