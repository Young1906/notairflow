"""
Sample for module's documentation

TODO:

"""

from .cores import Job
from .cores import Task

def test_job() -> Job:
    """
    Testing Job and Task's functionality
    - Should return error since there is a cycle within the job
    """
    with Job("test-job") as job:
        @Task.wrapper(job)
        def test01():
            print("x")

        @Task.wrapper(job)
        def test02():
            print("y")

        @Task.wrapper(job)
        def test03():
            print("z")

        # invalid job, contain cycle
        test01 >> test02
        test02 >> test03

    return job



if __name__ == "__main__":
    j = test_job()
    j()
