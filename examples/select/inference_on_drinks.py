import duckdb
import sys, os
sys.path.insert(0, os.path.abspath('.'))
from udf import *

# example use prompt/model specified
query = r"""
    SELECT drink, llm_model('ill provide you with a drink and a price in dollars, tell me in 1 word in the drink is cheap, average, or expensive', 'mistralai/Mistral-7B-Instruct-v0.3', drink, price)
    FROM read_csv('./datasets/drinks.csv')
"""
res = duckdb.sql(query).fetchall()
print(res)
# res: [('mocha', '.00\naverage'), ('flat white', '.50, average'), ('vanilla latte', '.50\n\nexpensive')]