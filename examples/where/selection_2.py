import duckdb
import sys, os
sys.path.insert(0, os.path.abspath('.'))
from udf import *
from timer import timing

@timing
def execute_query():
    query = r"""
        SELECT ProductName, description 
        FROM (SELECT * FROM read_csv('./datasets/clothing.csv') LIMIT 1)
        WHERE llm_task_local('trendy,classic,wont sell', 'zero-shot-classification', description) = 'trendy'
    """
    res = duckdb.sql(query).df()
    return res

@timing
def execute_query_batched():
    query = r"""
        SELECT ProductName, description 
        FROM (SELECT * FROM read_csv('./datasets/clothing.csv') LIMIT 20)
        WHERE llm_task_batch_local('trendy,classic,wont sell', 'zero-shot-classification', description) = 'trendy'
    """
    res = duckdb.sql(query).df()
    return res

print(execute_query())
print(execute_query_batched())