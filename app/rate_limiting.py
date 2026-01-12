from slowapi import Limiter
from slowapi.util import get_remove_address

limiter = Limiter(key_func=get_remove_address)