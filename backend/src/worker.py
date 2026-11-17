import time,json,traceback,os
from pathlib import Path
from sqlalchemy import select
from .config.database import Session
from .models.batch import Batch
from .models.sample import Sample
from .models.evidence import Evidence
from .services.batch import queue
from .services.query_engine import compare
from .services.correction import current_data_version,lock_and_read_data_version,racing_imports_overlap
ARTIFACTS=Path(os.getenv('ARTIFACTS_DIR','/artifacts'))
heartbeat=ARTIFACTS/'worker.heartbeat'
def run_one(batch_id):
    with Session() as db:
        batch=db.get(Batch,batch_id)
        if not batch or db.scalar(select(Evidence).where(Evidence.batch_id==batch_id)):return
        sample=db.get(Sample,batch.sample_id);batch.status='running';db.commit()
        # 执行期间不持有任何锁：长复核不拖住更正生效。仅记录起始数据版本。
        v_start=current_data_version(db)
        original_sql,candidate_sql=sample.original_sql,batch.candidate_sql
    try:
        result=compare(original_sql,candidate_sql);error=None
    except Exception as exc:
        result={'error':str(exc)[:500]};error=exc
    read_ids=result.pop('read_invoice_ids',None)
    with Session() as db:
        batch=db.get(Batch,batch_id)
        # 发布前短事务锁住版本行（FOR SHARE），与更正事务的 UPDATE 互斥：
        # 更正要么先提交（被 racing 检查发现），要么等本事务提交后生效（由证据失效判定兜底）。
        v_now=lock_and_read_data_version(db)
        if error:batch.status='failed'
        elif racing_imports_overlap(db,v_start,read_ids):batch.status='stale'
        else:batch.status='passed' if result['equivalent'] else 'different'
        db.add(Evidence(batch_id=batch_id,result=result,data_version=v_start,read_invoice_ids=read_ids))
        db.commit()
        (ARTIFACTS/f'review-{batch_id}.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
while True:
    try:
        heartbeat.write_text(str(time.time()))
        item=queue.blpop('query-review:jobs',timeout=2)
        if item:run_one(int(item[1]))
        else:
            with Session() as db:pending=list(db.scalars(select(Batch.id).where(Batch.status.in_(['queued','running']))))
            for ident in pending:run_one(ident)
    except Exception:traceback.print_exc();time.sleep(2)
