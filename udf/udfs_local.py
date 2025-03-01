from transformers import pipeline
import duckdb
from duckdb.typing import *
import pyarrow as pa

def llm_task_local(prompt, task, /, *attributes):
    n_attrs = len(attributes)
    if n_attrs < 1:
        return "error: must specify at least 1 attribute and a prompt and task" 
    
    if task == "summarization":
        summarizer = pipeline("summarization", model="facebook/bart-large-cnn")
        return summarizer(attributes[0], max_length=200, min_length=30, do_sample=False)[0]['summary_text']
    else:
        print(task, 'is unsupported')

def llm_task_batch_local(prompt, task, /, *attributes):
    n_attrs = len(attributes)
    if n_attrs < 1:
        return "error: must specify at least 1 attribute and a prompt and task" 
    
    task = task.to_pylist()[0]
    if task == "summarization":
        summarizer = pipeline("summarization", model="facebook/bart-large-cnn")
        res = summarizer(attributes[0].to_pylist(), max_length=200, min_length=30, do_sample=False)
        pa_arr = pa.array([res[i]['summary_text'] for i in range(len(attributes[0]))])
        return pa_arr
    else:
        print(task, 'is unsupported')

# register UDF
duckdb.create_function("llm_task_local", llm_task_local, return_type=VARCHAR)
duckdb.create_function("llm_task_batch_local", llm_task_batch_local, return_type=VARCHAR, type="arrow")