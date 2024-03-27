import networkx as nx
from matplotlib import pyplot as plt
from collections import defaultdict
from uuid import uuid4


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

        cycles_ = [
                (self.get_node_by_key(u), self.get_node_by_key(v)) 
                for (u, v) in cycles]
        
        # friendly name
        if cycles:
            return False

        return True

    def get_seq(self):
        seq = nx.topological_sort(self.G)
        return seq

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
        return f"<node> {self.name}"

    def get_key(self):
        return str(self.__key)
