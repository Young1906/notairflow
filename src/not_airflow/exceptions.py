"""
Possible exception in not_airflow
"""

class NotAirflowInvalidDAG(Exception):
    """
    When there is a cycle in the job
    """


class NotAirflowOutsideContext(Exception):
    """
    when job is modified outside the context manager
    """
