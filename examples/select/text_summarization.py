import duckdb
import sys, os
sys.path.insert(0, os.path.abspath('.'))
from udf import *


# example use prompt/task specified
query = r"""
    SELECT company, role, llm_task('please provide summary', 'summarization', description) 
    FROM read_csv('./datasets/jobs.csv')
"""
res = duckdb.sql(query).fetchall()
print(res)
# res: [('Google', ' Software Engineer III AI/ML', "{'summary_text': Google's software engineers develop the next-generation technologies that change how billions of users connect, explore, and interact with information and one another. The US base salary range for this full-time position is $136,000-$200,000 + bonus + equity + benefits. Our salary ranges are determined by role, level, and location.}"), ('Capital One', ' Senior AI Engineer', "{'summary_text': The Intelligent Foundations and Experiences (IFX) team is at the center of bringing our vision for AI at Capital One to life. In this role, you will: Partner with a cross-functional team of engineers, research scientists, technical program managers, and product managers to deliver AI-powered products.}")]