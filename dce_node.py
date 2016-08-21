import marshal
import types
import Pyro4
import sys
import base64


def deserialize_function(code_string):
    code = marshal.loads(code_string)
    func = types.FunctionType(code, globals(), "entry_point")
    return func


@Pyro4.expose
class Node:
    def __init__(self):
        self.entry_point = None

    def set_entry_point(self, params):
        code_string = base64.b64decode(params["data"])
        self.entry_point = deserialize_function(code_string)

    def execute(self, args=(), kwargs=None):
        if kwargs is None: kwargs = {}

        return self.entry_point(*args, **kwargs)


def main(args):
    node = Node()

    print("Start daemon")
    with Pyro4.Daemon(port=17888, host="localhost") as daemon:
        uri = daemon.register(node, "dce_node")
        print("Daemon started")
        daemon.requestLoop()

if __name__ == "__main__":
    main(sys.argv)
