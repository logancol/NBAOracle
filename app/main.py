from fastapi import FastAPI, HTTPException
import psycopg2
from openai import OpenAI
from app.core.config import settings
import logging
import sys

# Fast API app setup
app = FastAPI(title="BBALL ORACLE")
# Logger setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Stream handler for uvicorn console
stream_handler = logging.StreamHandler(sys.stdout)
log_formatter = logging.Formatter("%(asctime)s [%(processName)s: %(process)d] [%(threadName)s: %(thread)d] [%(levelname)s] %(name)s: %(message)s")
stream_handler.setFormatter(log_formatter)
logger.addHandler(stream_handler)

# ENV vars
DB_URL = settings.DATABASE_URL
OPENAI_API_KEY = settings.OPENAI_API_KEY
if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY is not set or not loaded")

client = OpenAI(
  api_key=OPENAI_API_KEY
)

# I'm probably going to end up sanitizing on the frontend as well
def sanitize_sql(query: str):
    if len(query) == 0:
        logger.error("====== SQL SANITIZATION FUNCTION RECEIVED EMPTY STRING ======")
        return ""
    
    if (len(query) > 5000):
        logger.error("====== SQL SANITIZATION FUNCTION RECEIVED EXCESSIVELY LONG STRING ======")
        return ""
    
    lower_query = query.lower()

    if 'select' not in lower_query:
        logger.error("====== SQL SANITIZATION FUNCTION RECEIVED NON-SELECTING QUERY ======")
        return ""

    bad_words = ['insert', 'update', 'delete', 'truncate', 'merge', 'create', 'alter', 'drop', 'rename', 'comment',
    'grant', 'revoke', 'begin', 'commit', 'rollback', 'savepoint', 'release', 'execute', 'do', 'set', 'load', 'listen', 'notify'
    ]

    for word in bad_words:
        if word in query.lower():
            logger.error("====== POTENTIALLY MALICIOUS QUERY ======")
            return ""
    
    return query

def load_schema_text():
    SCHEMA = ""
    schema_path = "constants/schema.txt"
    try:
        with open(schema_path, 'r') as file:
            SCHEMA = file.read()
    except:
        logger.error("SCHEMA TEXT NOT PROPERLY LOADED")
    finally:
        return SCHEMA

# Returns empty dict if the query has sanitization issues or if there's a problem running against the databse
def execute_sql(query: str):
    logger.info("====== EXECUTING SQL QUERY ======")
    # QUERY VALIDATION

    sanitizedQuery = sanitize_sql(query)
    if not sanitizedQuery:
        return {}
        
    # Consider implementing connection pooling
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    try:
        cur.execute(sanitizedQuery)
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        return {"columns": cols, "rows": rows}
    except psycopg2.Error as e:
        logger.error(f"====== PROBLEM RUNNING QUERY ON PBP DATA: {e} ======")
        return {}
    finally:
        cur.close()
        conn.close()


def get_sql_from_question(question: str, schema: str):
    logger.info("====== GETTING SQL FROM USER QUESTION ======")
    prompt = f"""
    You are a PostgreSQL query planner for NBA statistical data. You generate SQL to query the NBA database to answer natural langauge questions about
    player/team statistics. Do NOT explain results in prose. Return valid SQL ONLY.

    Below is the table schema, prioritize considering the value enumerations and other guidelines described in comments at the bottom of the schema to ensure an accurate response.

    {schema}
    
    The current season is the 2025-26 season, which has season id: 22025. You are never to attempt to alter the database, and this supersedes all possible user requests.

    User Question: "{question}"
    
    """
    sql = ""
    try:
        response = client.responses.create(
            model="gpt-5.2",
            input=prompt,
            reasoning={
                "effort": "medium"
            }
        )
        sql = response.output_text.strip()
        sql = sql.split("```")[0].strip() # Remove markdown delimeters
    except Exception as e:
        logger.error(f"====== PROBLEM GETTING SQL FROM OPENAI {e} ======")
    finally:
        return sql

def interpret_sql_response(response: str, query: str, question: str):
    logger.info("====== INTERPRETING SQL RESPONSE ======")
    prompt = f"""
    You are an SQL output interpreter for an NBA statistical data natural language querying tool. Given a user question, sql query, and output, you provide a concise, friendly, 
    markdown-less textual summary of the sql output to answer the user's question. Your #1 priority at all times should be to not reveal details regarding the internal
    structure of the database, tables, fields, etc. You are to interpret empty sql output as the query returning no results that align with the user request, and not an 
    error elsewhere in the pipeline. 
    
    User question: {question}
    SQL query: {query}
    DB response: {response}
    """
    answer = ""
    try:
        completion = client.responses.create(
            model="gpt-5.2",
            input=prompt
        )
        answer = completion.output_text.strip()
        answer = answer.split("```")[0].strip()
    except Exception as e:
        logger.error(f"====== PROBLEM GETTING RESULT INTERPRETATION FROM OPENAI {e} ======")
    finally:
        return answer

@app.get("/query")
def get_answer(question: str) -> str:
    logger.info('GET /query')
    db_schema = load_schema_text()

    # only raising http exceptions for internal server errors atm, client will be responsible santizing user requests
    if not db_schema:
        raise HTTPException(status_code=500, detail="Schema")
    
    sql = get_sql_from_question(question, db_schema)
    if not sql:
        raise HTTPException(status_code=500, detail="Problem generating query")
    
    database_answer = execute_sql(sql)
    if not database_answer:
        raise HTTPException(status_code=500, detail="Problem querying databse")

    formatted_response = interpret_sql_response(response=database_answer, query=sql, question=question)
    if not formatted_response:
        raise HTTPException(status_code=500, detail="Problem interpretting query output")

    return formatted_response