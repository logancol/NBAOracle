from fastapi import FastAPI, HTTPException
import psycopg2
from openai import OpenAI
from core.config import settings
import logging
import sys
from services.oracle import Oracle

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

try:
    client = OpenAI(
    api_key=OPENAI_API_KEY
    )
except Exception as e:
    logger.error(f"====== Problem creating OpenAI client ======")
    raise

schema_path = settings.SCHEMA_PATH
SCHEMA = ""
try:
    with open(schema_path, 'r') as file:
        SCHEMA = file.read()
except FileNotFoundError:
    logger.error(f"====== Schema file not found: {schema_path} ======")
    raise


@app.get("/query")
def get_answer(question: str) -> str:
    conn = psycopg2.connect(DB_URL)
    try:
        oracle = Oracle(logger=logger, schema=SCHEMA, client=client, conn=conn)
        return oracle.ask_oracle(question)
    finally:
        conn.close()

@app.post('/token')
def token():
    # finish implementing authentication logic
    # https://www.youtube.com/watch?v=I11jbMOCY0c 12:30
    return