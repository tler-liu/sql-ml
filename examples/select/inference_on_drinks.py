import duckdb
import sys, os
sys.path.insert(0, os.path.abspath('.'))
from udf import *
from timer import timing

@timing
def execute_query():
    query = r"""
        SELECT drink, llm_task_local('ill provide you with a drink and a price in dollars, tell me in 1 word in the drink is cheap, average, or expensive', 'text2text-generation', drink, price)
        FROM read_csv('./datasets/drinks.csv')
    """
    res = duckdb.sql(query).fetchall()
    return res

@timing
def execute_query_batched():
    query = r"""
        SELECT drink, llm_task_batch_local('ill provide you with a drink and a price in dollars, tell me in 1 word in the drink is cheap, average, or expensive', 'text2text-generation', drink, price)
        FROM read_csv('./datasets/drinks.csv')
    """
    res = duckdb.sql(query).fetchall()
    return res

print(execute_query())
print(execute_query_batched())



# res: [('mocha', '.00\naverage'), ('flat white', '.50, average'), ('vanilla latte', '.50\n\nexpensive')]