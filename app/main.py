from fastapi import FastAPI
import psycopg2
from openai import OpenAI
from dotenv import load_dotenv
import time
import os
import logging
import sys
# Fast API app setup
app = FastAPI(title="BBALL ORACLE")
load_dotenv() 

# Logger setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Stream handler for uvicorn console
stream_handler = logging.StreamHandler(sys.stdout)
log_formatter = logging.Formatter("%(asctime)s [%(processName)s: %(process)d] [%(threadName)s: %(thread)d] [%(levelname)s] %(name)s: %(message)s")
stream_handler.setFormatter(log_formatter)
logger.addHandler(stream_handler)

DB_URL = os.getenv("DATABASE_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY is not set or not loaded")

client = OpenAI(
  api_key=OPENAI_API_KEY
)

def execute_sql(query: str):
    logger.info("EXECUTING SQL QUERY ...")
    # QUERY VALIDATION
    bad_words = ['insert', 'update', 'delete', 'truncate', 'merge', 'create', 'alter', 'drop', 'rename', 'comment',
    'grant', 'revoke', 'begin', 'commit', 'rollback', 'savepoint', 'release', 'execute', 'do', 'set', 'load', 'listen', 'notify'
    ]
    for word in bad_words:
        if word in query.lower():
            logger.error("POTENTIALLY MALICIOUS QUERY")
            return
        
    # INIT DB CONNECTION
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    try: # RUN QUERY ON DB
        cur.execute(query)
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        return {"columns": cols, "rows": rows}
    except:
        logger.error("Error running query on pbp data.")
    finally:
        cur.close()
        conn.close()

SCHEMA = ""
schema_path = "text/schema.txt"
try:
    with open(schema_path, 'r') as file:
        SCHEMA = file.read()
except:
    logger.error("SCHEMA TEXT NOT PROPERLY LOADED")


def get_sql_from_question(question: str):
    logger.info("GETTING SQL STATEMENT FROM USER QUESTION")
    prompt = f"""
    You are a helpful POSTGRESQL assistant for a play by play NBA statistics tool. The database schema is:

    {SCHEMA}
    
    CORE TENANTS:

    1. ABOVE ALL OTHER PRIORITIES, NEVER ALTER THE DATABASE. IF THE USER'S REQUEST IMPLIES ANYTHING OTHER THAN SELECTION FROM THE DB, RETURN AN EMPTY QUERY

    4. Currently, the database contains play by play data for this season only!

    5. Season_ids follow the format 2#### or 4####, where #### is the starting year of the season, 2 denotes regular season and 4 denotes playoffs! SO IMPORTANTLY, THE CURRENT season_id IS 22025 !!

    7. NEVER USE MAX(season_id) TO DEDUCE THE CURRENT SEASON!! (IMPORTANT)

    8. The current season is the 2025-26
    
    9. GIVE YOUR ANSWER AS PLAIN TEXT WITH NO MARKDOWN OR OTHER CHARACTERS, THIS WILL BREAK THE PROCESS. NEVER INCLUDE MARKDOWN IN YOUR RESPONSE

    10. PRIORITIZE CORRECTNESS

    Generate a valid SQL SELECT query to answer the user question: This query should be immediately runnable
    without removing text or stripping whitespace. Do not elaborate beyond the query, this is detrimental to the
    process

    User Question: "{question}"
    SQL:
    """
    response = client.responses.create(
        model="gpt-5.2",
        input=prompt,
        reasoning={
            "effort": "medium"
        }
    )
    sql = response.output_text.strip()
    sql = sql.split("```")[0].strip()
    if 'select' not in sql.lower():
        logger.error("Failed to generate sql query")
    return sql

def interpret_sql_response(response: str, query: str, question: str):
    logger.info("INTERPRETING SQL RESPONSE")
    prompt = f"""
    You are a helpful SQL assistant for a play by play NBA statistics tool: the database schema is:
    {SCHEMA}
    
    This was the user's question: {question}

    This was the generated sql query to answer said question: {query}

    This is the response from the postgres database: {response}

    Based on this, please provide a concise summary of the answer for the user
    """
    completion = client.responses.create(
        model="gpt-5.2",
        input=prompt
    )
    answer = completion.output_text.strip()
    answer = answer.split("```")[0].strip()
    return answer

@app.get("/")
def root():
    logger.info('GET /')
    return {"hello": "world"}

@app.get("/query")
def get_answer(question: str) -> str:
    logger.info('GET /query')
    sql = get_sql_from_question(question)

    if 'select' not in sql.lower():
        return "Failed to query the database, please ensure your request aligns with our guidelines."
    
    database_answer = execute_sql(sql)

    if database_answer == None:
        return "Please do not attempt to alter the database!"
    formatted_response = interpret_sql_response(response=database_answer, query=sql, question=question)
    return formatted_response