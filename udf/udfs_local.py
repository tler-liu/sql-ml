from transformers import pipeline
import duckdb
from duckdb.typing import *
import pyarrow as pa
import numpy as np

def llm_task_local(prompt, task, /, *attributes):
    n_attrs = len(attributes)
    if n_attrs < 1:
        return "error: must specify at least 1 attribute and a prompt and task" 
    
    if task == "summarization":
        summarizer = pipeline("summarization", model="facebook/bart-large-cnn")
        return summarizer(attributes[0], max_length=200, min_length=30, do_sample=False)[0]['summary_text']
    elif task == "text2text-generation":
        generator = pipeline("text2text-generation", model="google/flan-t5-small")

        # format prompt
        llm_input = prompt + ": "
        for i in range(n_attrs):
            llm_input += str(attributes[i])
            if i != n_attrs - 1:
                llm_input += ", "
        
        return generator(llm_input, max_new_tokens=200, do_sample=False)[0]['generated_text']
    elif task == "zero-shot-classification": # only works with 1 attribute for now
        classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")
        labels = prompt.split(",")
        res = classifier(attributes[0], labels, multi_label=False, do_sample=False)
        return labels[np.argmax(res['scores'])]
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
    elif task == "text2text-generation":
        generator = pipeline("text2text-generation", model="google/flan-t5-small")

        # format prompts
        llm_input_vector = []
        prompt = prompt.to_pylist()[0]
        for row_idx in range(len(attributes[0])):
            llm_input = prompt + ": "
            for attr_idx in range(n_attrs):
                llm_input += str(attributes[attr_idx][row_idx])
                if attr_idx != n_attrs - 1:
                    llm_input += ", "
            llm_input_vector.append(llm_input)
        
        res = generator(llm_input_vector, max_new_tokens=200, do_sample=False)
        pa_arr = pa.array([res[i]['generated_text'] for i in range(len(attributes[0]))])
        return pa_arr
    elif task == "zero-shot-classification": # only works with 1 attribute for now
        classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")
        prompt = prompt.to_pylist()[0]
        labels = prompt.split(",")
        res = classifier(attributes[0].to_pylist(), labels, multi_label=False, do_sample=False)
        pa_arr = pa.array([labels[np.argmax(res[i]['scores'])] for i in range(len(attributes[0]))]) 
        return pa_arr
    else:
        print(task, 'is unsupported')

# register UDF
duckdb.create_function("llm_task_local", llm_task_local, return_type=VARCHAR)
duckdb.create_function("llm_task_batch_local", llm_task_batch_local, return_type=VARCHAR, type="arrow")