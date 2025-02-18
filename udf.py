import duckdb
from duckdb.typing import *
from huggingface_hub import InferenceClient
import os


# prompt/model specified UDF
def llm(prompt, model, /, *attributes):
    n_attrs = len(attributes)
    if n_attrs < 1:
        return "error: must specify at least 1 attribute and a prompt and model" 
    
    try:
        client = InferenceClient(
            model=model,
            token=os.environ.get("HUGGING_FACE_TOKEN"), # provide huggingface token
        )

        llm_prompt = prompt + "\n"
        
        for i in range(0, n_attrs):
            llm_prompt += str(attributes[i])
            if i != n_attrs - 1:
                llm_prompt += ", "
        
        return client.text_generation(llm_prompt)
    except:
        return "error: must specify a valid prompt and model"

# prompt/task specified UDF
def llm(prompt, task, /, *attributes):
    n_attrs = len(attributes)
    if n_attrs < 1:
        return "error: must specify at least 1 attribute and a prompt and model" 
    
    try:
        client = InferenceClient(
            model=task,
            token=os.environ.get("HUGGING_FACE_TOKEN"), # provide huggingface token
        )

        llm_prompt = prompt + "\n"
        
        for i in range(0, n_attrs):
            llm_prompt += str(attributes[i])
            if i != n_attrs - 1:
                llm_prompt += ", "
        
        return client.text_generation(llm_prompt)
    except:
        return "error: must specify a valid prompt and model"


# example use
duckdb.read_csv("drinks.csv")
duckdb.create_function("llm", llm, return_type=VARCHAR)
query = r"""
    SELECT drink, llm('ill provide you with a drink and a price in dollars, tell me in 1 word in the drink is cheap, average, or expensive', 'mistralai/Mistral-7B-Instruct-v0.3', drink, price)
    FROM 'drinks.csv'
"""
res = duckdb.sql(query).fetchall()
print(res)

# Notes:
# we should probably choose a model depending on the specific task
# perhaps specify the task in the llm function?