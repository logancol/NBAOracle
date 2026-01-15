import psycopg
from psycopg import AsyncConnection
from fastapi import HTTPException
from typing import Optional
from app.core.config import settings
from bcrypt import hashpw, gensalt, checkpw

DB_URL = settings.DATABASE_URL

def init_db(conn: psycopg.connection):
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                full_name VARCHAR(256),
                password_hash VARCHAR(256) NOT NULL,
                email VARCHAR(256) UNIQUE,
                created_at TIMESTAMP DEFAULT now()
            );
            """)

async def get_user_by_email(conn: AsyncConnection, email: str) -> Optional[dict]:
    async with conn.cursor() as cur:
        await cur.execute("SELECT password_hash, email, full_name FROM users WHERE email = %s", (email,))
        row = await cur.fetchone()
    if row:
        return {"password_hash": row[0], "email": row[1], "full_name": row[2]}
    return None

async def create_user(conn: AsyncConnection, password: str, email: str, full_name: str = ""):
    if not email:
        raise HTTPException(status_code=400, detail="Email required")
    password_hash = get_password_hash(password)
    try:
        async with conn.transaction():
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO users (full_name, password_hash, email) VALUES (%s, %s, %s, %s)",
                    (full_name, password_hash, email)
                )
    except psycopg.errors.UniqueViolation as e:
        raise HTTPException(status_code=409, detail="Email already being used.")
    except psycopg.Error as e:
        raise HTTPException(status_code=500, detail="DB Error when creating user.") from e
        
def get_password_hash(password: str):
    return hashpw(password.encode("utf-8"), gensalt()).decode("utf-8")