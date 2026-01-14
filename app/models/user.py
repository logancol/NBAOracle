from pydantic import BaseModel

class User(BaseModel):
    password_hash: str
    email: str
    full_name: str