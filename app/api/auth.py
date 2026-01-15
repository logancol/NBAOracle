from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from typing import Annotated
from datetime import timedelta
import logging 
from psycopg import AsyncConnection
from app.models.token import Token
from app.services.auth_service import authenticate_user, create_access_token
from app.db.db import get_async_conn

router = APIRouter(prefix='/auth', tags=['Auth'])

log = logging.getLogger(__name__)

@router.post("/login")
async def login_for_access_token(form_data: Annotated[OAuth2PasswordRequestForm, Depends()], conn: AsyncConnection = Depends(get_async_conn)) -> Token:
    # https://www.youtube.com/watch?v=I11jbMOCY0c&t=843s 13.54 finish following along almost there
    user = authenticate_user(email=form_data.username, password=form_data.password, conn=conn)
    if not user:
        raise HTTPException(
            status_code=401,
            detail="Incorrect email or password.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=1440)
    access_token = create_access_token(
        data={"sub": user['email;']}, expires_delta=access_token_expires
    )

    return Token(access_token, token_type="bearer")
    