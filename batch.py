import pyarrow.compute as pc
import duckdb
import pyarrow as pa
from huggingface_hub import InferenceClient
import os
import udf

# https://duckdb.org/2023/07/07/python-udf.html

# Built-In UDF
def add_built_in_type(x):
    return x + 1

# Arrow UDF
# this UDF batches rows 
def add_arrow_type(x, y):
    print(x, y)
    return pc.add(x,y)

# Registration
duckdb.create_function('add_built_in_type', add_built_in_type, ['BIGINT'], 'BIGINT', type='native')
duckdb.create_function('add_arrow_type', add_arrow_type, return_type='BIGINT', type='arrow')

# Integer View with 10,000,000 elements.
duckdb.sql("""
     SELECT i
     FROM range(10000) tbl(i);
""").to_view("numbers")

# Calls for both UDFs
native_res = duckdb.sql("SELECT sum(add_built_in_type(i)) FROM numbers").fetchall()
arrow_res = duckdb.sql("SELECT sum(add_arrow_type(i, i)) FROM numbers").fetchall()



# client = InferenceClient(
#     model="facebook/bart-large-cnn",
#     token=os.environ.get("HUGGING_FACE_TOKEN"), # provide huggingface token
# )

# print(client.summarization(['At Capital One, we are creating responsible and reliable AI systems, changing banking for good. For years, Capital One has been an industry leader in using machine learning to create real-time, personalized customer experiences. Our investments in technology infrastructure and world-class talent — along with our deep experience in machine learning — position us to be at the forefront of enterprises leveraging AI. From informing customers about unusual charges to answering their questions in real time, our applications of AI & ML are bringing humanity and simplicity to banking. We are committed to continuing to build world-class applied science and engineering teams to deliver our industry leading capabilities with breakthrough product experiences and scalable, high-performance AI infrastructure. At Capital One, you will help bring the transformative power of emerging AI capabilities to reimagine how we serve our customers and businesses who have come to love the products and services we build.', 'Google Ads is helping power the open internet with the best technology that connects and creates value for people, publishers, advertisers, and Google. We’re made up of multiple teams, building Google’s Advertising products including search, display, shopping, travel and video advertising, as well as analytics. Our teams create trusted experiences between people and businesses with useful ads. We help grow businesses of all sizes from small businesses, to large brands, to YouTube creators, with effective advertiser tools that deliver measurable results. We also enable Google to engage with customers at scale.']))


# batching example
query = r"""
    SELECT drink, llm_batch('ill provide you with a drink and a price in dollars, tell me in 1 word in the drink is cheap, average, or expensive', 'mistralai/Mistral-7B-Instruct-v0.3', drink, price)
    FROM read_csv('./datasets/drinks.csv')
"""
res = duckdb.sql(query).fetchall()
print(res)