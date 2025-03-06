import duckdb
import sys, os
sys.path.insert(0, os.path.abspath('.'))
from udf import *
from timer import timing

@timing
def execute_query():
    query = r"""
        SELECT llm_task_batch_local('summarize', 'summarization', description) AS summary
        FROM read_csv('./datasets/jobs.csv')
        WHERE summary LIKE '%AI%' or summary LIKE '%ML%'
    """
    res = duckdb.sql(query).df()
    return res

@timing
def execute_query_optimized():
    query = r"""
        SELECT summary
        FROM (
            SELECT llm_task_batch_local('summarize', 'summarization', description) AS summary
            FROM read_csv('./datasets/jobs.csv')
        )
        WHERE summary LIKE '%AI%' or summary LIKE '%ML%'
    """
    res = duckdb.sql(query).df()
    return res

print(execute_query())
print(execute_query_optimized())