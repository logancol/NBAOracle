from openai import OpenAI
from app.core.config import settings
import logging
import sys
from contextlib import asynccontextmanager
from app.db.db import get_async_pool_ro, get_async_pool_ar, get_async_pool_rw
from app.api.questions import router as questions_router
from app.api.auth import router as auth_router
from fastapi import FastAPI
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi import _rate_limit_exceeded_handler
from app.services.rate_limiting import limiter

# Global logging config for api
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s [%(processName)s: %(process)d] [%(threadName)s: %(thread)d] "
           "[%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    OPENAI_API_KEY = settings.OPENAI_API_KEY
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not set or not loaded")
    if not settings.DATABASE_URL:
        raise RuntimeError("RO DATABASE URL NOT SET OR LOADED")
    if not settings.DATABASE_URL_RW:
        raise RuntimeError("RW DATABASE URL NOT SET OR LOADED")
    if not settings.DATABASE_URL_AUTHR:
        raise RuntimeError("AR DATABASE URL NOT SET OR LOADED")
    
    # instantiating openai client as part of global state
    app.state.openai_client = OpenAI(api_key=OPENAI_API_KEY)

    schema_path = settings.SCHEMA_PATH
    try: 
        with open(schema_path, 'r') as file:
            app.state.schema = file.read()
    except FileNotFoundError:
        logger.error(f"Schema file not found: {schema_path}")
        return
    await get_async_pool_ro().open()
    await get_async_pool_rw().open()
    await get_async_pool_ar().open()
    try:
        yield # yield til end of life span
    finally:
        await get_async_pool_rw().close()
        await get_async_pool_ro().close()
        await get_async_pool_ar().close()
        
app = FastAPI(lifespan=lifespan, title="BBALL ORACLE")

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

app.include_router(questions_router)
app.include_router(auth_router)