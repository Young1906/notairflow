from functools import partial
from functools import wraps

from NOTAIRFLOW.classes import Job
from NOTAIRFLOW.classes import Task


def task(task_name: str):
    @wraps()
    def inner(func):
        def innerr(*args, **kwargs):


