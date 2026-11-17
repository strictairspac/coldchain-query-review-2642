from sqlalchemy import select
from fastapi import HTTPException
from ..models.evidence import Evidence
from ..models.correction import Import
def _validity_map(db,records):
    """证据在其数据版本之后、被触及读集的已生效更正置为失效；未受影响保持有效。"""
    imports=[(x.data_version,set(x.affected_invoice_ids or []),x.id) for x in db.scalars(select(Import).where(Import.status=='applied'))]
    out={}
    for ev in records:
        stale,by=False,[]
        if ev.data_version is not None and ev.read_invoice_ids is not None:
            read=set(ev.read_invoice_ids)
            for version,affected,imp_id in imports:
                if version>ev.data_version and read&affected:stale=True;by.append(imp_id)
        out[ev.id]=(stale,sorted(by))
    return out
def serialize(x,stale=False,invalidated_by=None):
    return {'id':x.id,'batch_id':x.batch_id,'result':x.result,'data_version':x.data_version,'stale':stale,'invalidated_by':invalidated_by or []}
def list_evidence(db):
    records=list(db.scalars(select(Evidence).order_by(Evidence.id.desc())))
    validity=_validity_map(db,records)
    return [serialize(x,*validity[x.id]) for x in records]
def get_evidence(db,batch_id):
    x=db.scalar(select(Evidence).where(Evidence.batch_id==batch_id))
    if not x:raise HTTPException(404,'证据尚未生成，请等待 worker 完成')
    stale,by=_validity_map(db,[x])[x.id]
    return serialize(x,stale,by)
