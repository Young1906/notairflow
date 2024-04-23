import inspect
import warnings
from collections import defaultdict
from functools import wraps
from uuid import uuid4

import networkx as nx


class Graph:
    def __init__(self):
        self.G = nx.DiGraph()
        
        # keep track of the node by id
        self.nodes = {}

    def add_node(self, n):
        self.G.add_node(n.get_key())
        self.nodes[n.get_key()] = n

    def add_edge(self, u, v):
        self.G.add_edge(u.get_key(), v.get_key())

    def get_node_by_key(self, key):
        return self.nodes[key]

    # check if the execution graph is valid
    def is_valid(self):
        # check for circle
        try:
            cycles = nx.find_cycle(self.G)

        except nx.exception.NetworkXNoCycle:
            cycles = []

        # cycles_ = [
        #         (self.get_node_by_key(u), self.get_node_by_key(v)) 
        #         for (u, v) in cycles]
        
        # friendly name
        if cycles:
            return False

        return True

    def get_seq(self):
        seq = nx.topological_sort(self.G)
        return list(seq)


class Node:
    def __init__(self, name: str, g: Graph):
        self.name = name
        self.__key = uuid4()

        # register the node to graph G
        self.g = g
        self.g.add_node(self)

    def __lshift__(self, other):
        self.g.add_edge(other, self)

    def __rshift__(self, other):
        other << self

    def __repr__(self):
        return f"((node) {self.name})"

    def get_key(self):
        return str(self.__key)


class Job(Graph):
    def __init__(self, name: str):
        super().__init__()
        self.name = name

    def __call__(self):

        # Get execution sequence: return a sequence of task's id
        seq = self.get_seq()

        for task in seq:
            task = self.get_node_by_key(task)
            code, msg = task()

            if code != 0:
                raise ValueError(msg)
                return code, msg

        return 0, "success"


    def __enter__(self):
        return self

    def __exit__(self, *args):
        is_valid = self.is_valid()
        if not is_valid:
            raise ValueError("Invalid DAG, found cycle in DAG")


class Task(Node):
    def __init__(self, job: Job, name: str, f: callable):
        super().__init__(name=name, g=job)

        # verify function signature: no params can be passed into f()
        spec = inspect.getargspec(f)
        
        if len(spec.args) or spec.varargs or spec.keywords:
            raise NotImplementedError("Can't pass anything into f() yet")

        self.f = self.wrap(f)

    @staticmethod
    def wrap(f):
        def __inner__():
            try:
                ret = f() 
                if ret:
                    # f() must be self-contained, no args can be passed
                    # no result can be returned
                    warnings.warn(f"Task {self.name} return data, this will be discard")
                return 0, None
            except Exception as e:
                return 1, str(e)
        return __inner__

    @staticmethod
    def task_wrapper(job):
        def inner(func):
            return Task(job, func.__name__, func)

        return inner

    def __call__(self): return self.f()
