from pydantic import BaseModel
class EvidenceOutput(BaseModel):
    id:int
    batch_id:int
    result:dict
    data_version:int|None=None
    stale:bool=False
    invalidated_by:list[int]=[]
