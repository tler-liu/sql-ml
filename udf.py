import duckdb
from duckdb.typing import *
from huggingface_hub import InferenceClient
import os

client = InferenceClient(
    model="mistralai/Mistral-7B-Instruct-v0.3",
    token=os.environ.get("HUGGING_FACE_TOKEN"), # provide huggingface token
)

def llm(*args):
    n_args = len(args)
    if n_args < 2:
        return "error: must specify at least 2 args" 

    llm_prompt = args[-1] + "\n"
    
    for i in range(0, n_args - 1):
        llm_prompt += str(args[i])
        if i != n_args - 2:
            llm_prompt += ", "
    
    return client.text_generation(llm_prompt)


# example use
duckdb.read_csv("drinks.csv")
duckdb.create_function("llm", llm, return_type=VARCHAR)
query = r"""
    SELECT drink, llm(drink, price, 'ill provide you with a drink and a price in dollars, tell me in 1 word in the drink is cheap, average, or expensive')
    FROM 'drinks.csv'
"""
res = duckdb.sql(query).fetchall()
print(res)

# Notes:
# we should probably choose a model depending on the specific task
# perhaps specify the task in the llm function?


