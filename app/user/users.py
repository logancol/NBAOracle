import psycopg2
from fastapi import HTTPException
from typing import Optional
import os
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

def init_db():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    conn.execute("""
    CREATE TABLE IF NOT EXISTS user (
        id SERIAL PRIMARY KEY,
        full_name VARCHAR(256),
        username VARCHAR(256) UNIQUE NOT NULL,
        password_hash VARCHAR(256) NOT NULL,
        email VARCHAR(256) UNIQUE,
        created_at TIMESTAMP DEFAULT now()
    );
    """)
    conn.commit()
    cur.close()
    conn.close()

def get_user(username: str) -> Optional[dict]:
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("SELECT username, password_hash, email, full_name FROM users WHERE username = %s", username)
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row:
       return {"username": row[0], "password_hash": row[1], "email": row[2], "full_name": row[3]}
    return None

def create_user(username: str, password_hash: str, email: str = "", full_name: str = ""):
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    try: 
        cur.execute(
            "INSERT INTO user (username, full_name, password_hash, email) VALUES (%s, %s, %s, %s,)",
            (username, full_name, password_hash, )
        )
        cur.commit()
    except psycopg2.Error as e:
        raise HTTPException(status_code=409, detail="This username is already in use.")
    finally: 
        conn.close()