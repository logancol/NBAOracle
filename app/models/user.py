from pydantic import BaseModel, ConfigDict, EmailStr, field_validator, Field
from typing import Optional
import re

class UserBase(BaseModel):
    model_config = ConfigDict(extra="forbid")
    email: EmailStr
    full_name: Optional[str] = Field(max_length=256)

class UserCreate(UserBase):
    password: str

    @field_validator("password")
    @classmethod
    def password_strength(cls, v: str) -> str: 
        if len(v) < 8:
            raise ValueError("Password should be at least 8 characters")
        if not re.search(r"[^\w\s]", v):
            raise ValueError("Password must contain a special character")
        if len(v) > 50:
            raise ValueError("Password too long")
        return v
    model_config = ConfigDict(extra="forbid")

class UserPublic(UserBase):
    model_config = ConfigDict(extra="forbid")

class UserInDB(UserBase):
    model_config = ConfigDict(extra="forbid")
    password_hash: str