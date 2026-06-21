from pydantic import BaseModel

class OpPayload(BaseModel):
    op: str  # A operação a replicar, p.ex. "RESERVE 12A passenger=P1"

class PrepareArgs(BaseModel):
    view: int
    op_num: int
    op: str
    commit_num: int
    client_id: int
    request_num: int

class PrepareOKArgs(BaseModel):
    view: int
    op_num: int
    replica_id: int

class CommitArgs(BaseModel):
    view: int
    commit_num: int

class RequestArgs(BaseModel):
    op: str
    client_id: int
    request_num: int