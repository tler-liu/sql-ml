import duckdb
import sys, os
sys.path.insert(0, os.path.abspath('.'))
from udf import *

query = r"""
    SELECT company, role
    FROM read_csv('./datasets/jobs.csv')
    WHERE llm_task('where is this job located?', 'question-answering', description) LIKE '%US%'
"""
res = duckdb.sql(query).fetchall()
print(res)