# NOTAIRFLOW



## Installtion

```
pip install git+https://github.com/young1906/notairflow
```

## Examples

```python
from NOTAIRFLOW import Task, Job


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
            print("z")

        # invalid job, contain cycle
        test01 >> test02
        test02 >> test03
        test01 << test03

    return job



if __name__ == "__main__":
    j = TestJob()
    j()
```
