from .graph import Node, Graph

class Job(Graph):
    def __init__(self, name: str):
        super().__init__()
        self.name = name

    def execute(self):
        assert self.is_valid(), ValueError("Invalid DAG")

        # Get execution sequence: return a sequence of task's id
        seq = self.get_seq()

        for task in seq:
            task = self.get_node_by_key(task)
            code, msg = task.f()

            if code != 0:
                raise ValueError(msg)
                break




class Task(Node):
    def __init__(self, job: Job, name: str, f: callable):
        super().__init__(name=name, g=job)
        self.f = self.wrap(f)

    def wrap(self, f):
        def __inner__(*args, **kwargs):
            try:
                f()
                return 0, None
            except Exception as e:
                return 1, str(e)
        return __inner__ 


job = Job("test")
task0 = Task(job, "task0", lambda: 1/0)
task1 = Task(job, "task1", lambda: print("1"))
task2 = Task(job, "task2", lambda: print("2"))


task0 << task1
task1 >> task2

job.execute()
