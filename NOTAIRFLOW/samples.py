import networkx as nx
from matplotlib import pyplot as plt

from NOTAIRFLOW.not_airflow import Job
from NOTAIRFLOW.not_airflow import Task

def TestJob() -> Job:
    with Job("test-job") as job:
        @Task.task_wrapper(job)
        def test01():
            print("x")

        @Task.task_wrapper(job)
        def test02():
            print("y")

        @Task.task_wrapper(job)
        def test03():
            print("z")

        # invalid job, contain cycle
        test01 >> test02
        test02 >> test03

    return job



if __name__ == "__main__":
    j = TestJob()
    j()
