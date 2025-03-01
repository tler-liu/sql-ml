import duckdb
import sys, os
sys.path.insert(0, os.path.abspath('.'))
from udf import *

query = r"""
    SELECT id, name, llm_task('give me the airport code of the nearest airport to the given location. answer with just the code', 'text-generation', location)
    FROM read_csv('./datasets/employees.csv')
    WHERE location <> 'New York NY'
"""

res = duckdb.sql(query).fetchall()
print(res)