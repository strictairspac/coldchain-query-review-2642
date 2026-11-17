from pydantic import BaseModel,Field
class ImportInput(BaseModel):
    file_name:str=Field(min_length=1,max_length=200)
    content:str=Field(min_length=1,max_length=200000)
