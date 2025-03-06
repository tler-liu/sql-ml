import duckdb
import sys, os
sys.path.insert(0, os.path.abspath('.'))
from udf import *
from timer import timing

@timing
def execute_query_batched():
    query = r"""
        
    """
    res = duckdb.sql(query).df()
    return res

print(execute_query_batched())