from transformers import pipeline
import duckdb
from duckdb.typing import *

def llm_task_local(prompt, task, /, *attributes):
    n_attrs = len(attributes)
    if n_attrs < 1:
        return "error: must specify at least 1 attribute and a prompt and task" 
    
    if task == "summarization":
        summarizer = pipeline("summarization", model="facebook/bart-large-cnn")
        return summarizer(attributes[0], max_length=200, min_length=30, do_sample=False)[0]['summary_text']

# register UDF
duckdb.create_function("llm_task_local", llm_task_local, return_type=VARCHAR)