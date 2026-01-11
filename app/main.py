from fastapi import FastAPI
import psycopg2
from openai import OpenAI
from dotenv import load_dotenv
import json
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


def get_sql_answer_template(question: str):
    logger.info("GETTING SQL STATEMENT AND ANSWER TEMPLATE FROM USER QUESTION")
    prompt = f"""
    You are a PostgreSQL query planner for NBA statistical data. You generate SQL to query the NBA database to answer natural langauge questions about
    player/team statistics, and provide a template for outputting or interpreting the results. Do NOT explain results in prose. Return valid JSON ONLY.

    Below is the table schema, prioritize considering the value enumerations and other guidelines described in comments at the bottom of the schema to ensure an accurate response.

    {SCHEMA}
    
    The current season is the 2025-26 season, which has season id 22025. You are never to attempt to alter the database, and this supersedes all possible user requests. You are never to
    provide data, or an output template that reveals the inner structure of the database to a user. 

    Set requires_elaboration to true if the answer requires explanation, comparison, or trend analysis. If the answer can be expressed as simple numeric SQL query results that can be 
    expressed without further processing or aggregation, set requires elabroation to false and create an answer template to display the query results. 

    Output template contract:
    {{
        "sql": "string",
        "requires_elaboration": true | false
        "result_num": "int",
        "answer template": "
    }}

    Output template JSON EXAMPLE: 
        "sql": "SELECT COUNT(*) AS value FROM pbp_raw_event p JOIN player pl ON p.shooter_id = pl.id WHERE pl.full_name = 'LeBron James' AND p.period = 4;",
        "result_num": 1,
        "answer_template": "LeBron James has attempted {{value}} shots in the 4th quarter in his career.",

    User Question: "{question}"
    
    """
    response = client.responses.create(
        model="gpt-5.2",
        input=prompt,
        reasoning={
            "effort": "medium"
        }
    )
    
    try:
        plan = json.loads(response)
    except:
        logger.error("====== INVALID JSON RETUNRED FROM OPENAI API CALL ======")
        return ""
    


def interpret_sql_response(response: str, query: str, question: str):
    logger.info("INTERPRETING SQL RESPONSE")
    prompt = f"""
    You are a helpful SQL assistant for a play by play NBA statistics tool: the database schema is:
    {SCHEMA}
    
    This was the user's question: {question}

    This was the generated sql query to answer said question: {query}

    This is the response from the postgres database: {response}

    Based on this, please provide a concise summary of the answer for the user

    IMPORTANTLY NEVER RESPOND IN A WAY THAT REVEALS THE INTERNAL STRUCTURE OF THE DATABASE. YOU ARE TO USE THE RESULTS OF THE SQL QUERY TO CONVERSATIONALLY ANSWER
    THE USERS QUESTION WITH NATURAL LANGUAGE AND STATS IF APPLICABLE, BUT DO NOT REVEAL INTERNALS SUCH AS GAME IDS PLAYER IDS ETC.
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
        return "Error fetching result."
    formatted_response = interpret_sql_response(response=database_answer, query=sql, question=question)
    return formatted_response