import duckdb
import sys, os
sys.path.insert(0, os.path.abspath('.'))
from udf import *
from timer import timing

# this is using the inference api and not local model
@timing
def execute_query():
    query = r"""
    SELECT company, role
    FROM read_csv('./datasets/jobs.csv')
    WHERE llm_task('where is this job located?', 'question-answering', description) LIKE '%US%'
    """
    res = duckdb.sql(query).df()
    return res

print(execute_query())