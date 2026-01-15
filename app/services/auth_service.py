from app.core.config import settings
from typing import Annotated
from fastapi.security import OAuth2PasswordBearer
from fastapi import HTTPException, Depends
from app.db.db import get_async_conn
from psycopg import AsyncConnection
from models.token import TokenData
import datetime
import jwt
from jwt.exceptions import InvalidTokenError
from StreamD.app.services.user_service import get_user_by_email
from app.utils.auth_utils import verify_password
from models.user import User

JWT_SECRET_KEY = settings.JWT_SECRET_KEY
ALGORITHM = "HS256"

oauth2_scheme = OAuth2PasswordBearer("/auth/login")

async def authenticate_user(password: str, email: str, conn: AsyncConnection):
    if not email or not password:
        return None
    user = await get_user_by_email(email, conn=conn)
    if not user:
        return None
    if not verify_password(plain_text=password, hashed_password=user['password_hash']):
        return False
    return user

def create_access_token(data: dict, expires_delta: datetime.timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(datetime.timezone.utc) + expires_delta
    else:
        expire = datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)], conn: AsyncConnection = Depends(get_async_conn)):
    credentials_exception = HTTPException(
        status_code = 401,
        detail="Could not validate user credentials",
        headers={"WWW-Authenticate": "Bearer"}
    )
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[ALGORITHM])
        email = payload.get("sub")
        if not email:
            raise credentials_exception
        token_data = TokenData(email=email)
    except InvalidTokenError:
        raise credentials_exception
    user = get_user_by_email(conn=conn, email=token_data.email)
    if user is None:
        raise credentials_exception
    return user

async def get_current_active_user(current_user: User = Depends(get_current_user)):
    return current_user