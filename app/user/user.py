from pydantic import BaseModel

class User(BaseModel):
    username: str
    password_hash: str
    email: str
    full_name: str