import duckdb
import sys, os
sys.path.insert(0, os.path.abspath('.'))
from udf import *
from timer import timing

@timing
def execute_query():
    query = r"""
        SELECT company, role, llm_task_local('please provide summary', 'summarization', description) 
        FROM read_csv('./datasets/jobs.csv')
    """
    res = duckdb.sql(query).fetchall()
    return res

print(execute_query())