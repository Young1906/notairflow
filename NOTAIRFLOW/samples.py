import networkx as nx
from matplotlib import pyplot as plt

from NOTAIRFLOW.cores import Job
from NOTAIRFLOW.cores import Task

def TestJob() -> Job:
    with Job("test-job") as job:
        @Task.wrapper(job)
        def test01():
            print("x")

        @Task.wrapper(job)
        def test02():
            print("y")

        @Task.wrapper(job)
        def test03():
            1/0
            print("z")

        # invalid job, contain cycle
        test01 >> test02
        test02 >> test03

    return job



if __name__ == "__main__":
    j = TestJob()
    j()
