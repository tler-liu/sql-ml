import duckdb
from duckdb.typing import *
from huggingface_hub import InferenceClient
import os
import sys

# prompt/model specified UDF
def llm_model(prompt, model, /, *attributes):
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
def llm_task(prompt, task, /, *attributes):
    n_attrs = len(attributes)
    if n_attrs < 1:
        return "error: must specify at least 1 attribute and a prompt and model" 
    

    model = ""
    if task == "text-generation":
        model = "mistralai/Mistral-7B-Instruct-v0.3"
    elif task == "text-classification":
        model = "ProsusAI/finbert"
    elif task == "summarization":
        model = "facebook/bart-large-cnn"
    elif task == "question-answering":
        model = "distilbert/distilbert-base-cased-distilled-squad"
    else:
        return "error: unsupported task"

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
        
        if task == "text-generation":
            return client.text_generation(llm_prompt)
        elif task == "text-classification":
            return client.text_classification(llm_prompt)
        elif task == "summarization":
            return client.summarization(llm_prompt)['summary_text']
        elif task == "question-answering":
            return client.question_answering(question=prompt, context=attributes[0])['answer']
        else:
            return "error: unsupported task"
    except Exception as e:
        print(e)
        return "error: must specify a valid prompt and task"


# register UDFs
duckdb.create_function("llm_model", llm_model, return_type=VARCHAR)
duckdb.create_function("llm_task", llm_task, return_type=VARCHAR)
