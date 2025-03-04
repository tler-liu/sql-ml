import duckdb
import sys, os
sys.path.insert(0, os.path.abspath('.'))
from udf import *
from timer import timing

@timing
def execute_query():
    query = r"""
        SELECT id, name, llm_task_local('what is the 3 letter airport code of the cloest airport to', 'text2text-generation', location) as airport_code
        FROM read_csv('./datasets/employees.csv')
        WHERE location <> 'New York NY'
    """
    res = duckdb.sql(query).df()
    return res

@timing
def execute_query_batched():
    query = r"""
        SELECT id, name, llm_task_batch_local('what is the 3 letter airport code of the cloest airport to', 'text2text-generation', location) as airport_code
        FROM read_csv('./datasets/employees.csv')
        WHERE location <> 'New York NY'
    """
    res = duckdb.sql(query).df()
    return res


print(execute_query())
print(execute_query_batched())