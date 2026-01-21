from pydantic import BaseModel, Field

class AnswerBase(BaseModel):
    answer: str = Field(max_length = 250)

class QuestionBase(BaseModel):
    question: str = Field(max_length = 250)

class AnswerResponse(AnswerBase):
    pass

