from fastapi import APIRouter, Depends, HTTPExcetion, status
from fastapi.security import OAuth2PasswordRequestForm
from typing import Annotated
import logging 
from psycopg import AsyncConnection
from app.models.token import Token
from app.db.db import get_async_conn

router = APIRouter(prefix='/auth', tags=['Auth'])

log = logging.getLogger(__name__)

@router.post("/login")
async def login_for_access_token(form_data: Annotated[OAuth2PasswordRequestForm, Depends()], conn: AsyncConnection = Depends(get_async_conn)) -> Token:
    # https://www.youtube.com/watch?v=I11jbMOCY0c&t=843s 13.54 finish following along almost there
    