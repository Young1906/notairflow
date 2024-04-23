import unittest
from not_airflow import Task, Job


def build_success_job () -> Job:
    """
    simple graph task01 -> task02 -> task03
    """
    with Job("test-job") as job:
        @Task.wrapper(job)
        def test01():
            pass

        @Task.wrapper(job)
        def test02():
            pass

        @Task.wrapper(job)
        def test03():
            pass

        # invalid job, contain cycle
        test01 >> test02
        test02 >> test03

    return job


def build_cycle_job () -> Job:
    """
    simple graph task01 -> task02 -> task03
    """
    with Job("test-job") as job:
        @Task.wrapper(job)
        def test01():
            pass

        @Task.wrapper(job)
        def test02():
            pass

        @Task.wrapper(job)
        def test03():
            pass

        # invalid job, contain cycle
        test01 >> test02
        test02 >> test03
        test01 << test03

    return job

class SuccessTest(unittest.TestCase):
    """
    """
    def setUp(self):
        self.job_success = build_success_job()
        self.job_fail_cycle = build_cycle_job()

    def test_success(self):
        code, msg = self.job_success()
        self.assertEqual(code, 0)


    def test_cycle_job (self):
        code, msg = self.job_fail_01()
        self.assertEqual(code, 1)







if __name__ == "__main__":
    unittest.main()

