"""
core's implementation of not_airflow

"""

import inspect
import warnings

# from collections import defaultdict
# from functools import wraps
from uuid import uuid4

import networkx as nx

from .exceptions import NotAirflowInvalidDAG
from .exceptions import NotAirflowOutsideContext


class Job:
    """
    job container
    """

    def __init__(self, name: str):
        self.name = name
        self.job_graph = nx.DiGraph()

        # keep track of the node by id
        self.nodes = {}
        self.freeze = False

    def add_node(self, n):
        """
        register a note to job's graph
        """
        if self.freeze:
            raise NotAirflowOutsideContext()

        self.job_graph.add_node(n.get_key())
        self.nodes[n.get_key()] = n

    def add_edge(self, u, v):
        """
        register an edge to job's graph
        """
        if self.freeze:
            raise NotAirflowOutsideContext()

        self.job_graph.add_edge(u.get_key(), v.get_key())

    def get_node_by_key(self, key):
        """
        return a node by its key
        """
        return self.nodes[key]

    # check if the execution graph is valid
    def is_valid(self):
        """
        Check if the job is valid, must contain no cycle within the graph
        """
        try:
            cycles = nx.find_cycle(self.job_graph)

        except nx.exception.NetworkXNoCycle:
            cycles = []

        if cycles:
            return False

        return True

    def get_seq(self):
        """
        Convert execution graph into a queue so task can be execute
        sequentially
        """
        seq = nx.topological_sort(self.job_graph)
        return list(seq)

    def __call__(self):
        # Get execution sequence: return a sequence of task's id
        seq = self.get_seq()

        for task in seq:
            task = self.get_node_by_key(task)
            code, msg = task()

            if code != 0:
                # raise ValueError(msg)
                warnings.warn(f"Task {task.name} failed!!! Err msg: {msg}")

                # interrupt the sequence and exit
                return code, msg

        return 0, "success"

    def __enter__(self):
        return self

    def __exit__(self, *args):
        is_valid = self.is_valid()

        # can't add more job outsize the context
        self.freeze = True

        if not is_valid:
            raise NotAirflowInvalidDAG()


class Task:
    """
    task container
    """

    def __init__(self, job: Job, name: str, f: callable):
        self.name = name
        self.__key = uuid4()

        # register the node to graph G
        self.g = job
        self.g.add_node(self)

        # verify function signature: no params can be passed into f()
        spec = inspect.getargspec(f)

        if len(spec.args) or spec.varargs or spec.keywords:
            raise NotImplementedError("Can't pass anything into f() yet")

        self.f = self.wrap(f)

    def __lshift__(self, other):
        self.g.add_edge(other, self)

    def __rshift__(self, other):
        other << self

    def __repr__(self):
        return f"((node) {self.name})"

    def get_key(self):
        """
        return task's key
        """
        return str(self.__key)

    @staticmethod
    def wrap(f):
        """
        tranform f signature f(any) -> any() into f() -> code, msg
        """

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
    def wrapper(job):
        """
        Convert any callable func into task instance,
        for syntax convenience
        """

        def inner(func):
            return Task(job, func.__name__, func)

        return inner

    def __call__(self):
        return self.f()
