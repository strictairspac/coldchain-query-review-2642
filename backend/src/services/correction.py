"""账单更正文件导入：版本递增、作废终态、整批原子生效或整批拒绝。

CSV 列：invoice_id,version,customer,route,amount,temperature,status,void
- 同号仅采用更高版本；旧版本直接跳过（乱序保护）。
- 同版本同内容幂等跳过；同版本异内容整批拒绝（回滚本批全部写入）。
- 作废为终态：作废后任何版本都不再复活该账单；作废重放幂等。
- 相同文件内容（SHA-256）重复导入返回首个处理记录，不重复写账。
- 逐账单 pg_advisory_xact_lock 按序加锁，并发交叉更正由数据库串行化，不丢新值。
- 每批生效后 data_version 递增，供复核证据失效判定。
"""
import csv,hashlib,io
from decimal import Decimal,InvalidOperation
from sqlalchemy import select,text
from sqlalchemy.exc import IntegrityError
from ..models.correction import Import,ImportItem
from ..models.invoice import Invoice

FIELDS=('invoice_id','version','customer','route','amount','temperature','status','void')
VOID_TRUE={'true','1','yes','y','是'}
VOID_FALSE={'false','0','no','n','否',''}
MAX_ROWS=5000

class RejectBatch(Exception):pass

def ensure_meta(db):
    """在任何业务锁之前保证版本行存在；ON CONFLICT 等待并发插入提交，避免与顾问锁形成循环等待。"""
    db.execute(text("INSERT INTO meta(key,value) VALUES ('data_version','1') ON CONFLICT(key) DO NOTHING"))
    db.flush()

def current_data_version(db):
    row=db.execute(text("SELECT value FROM meta WHERE key='data_version'")).scalar()
    return int(row) if row is not None else 1

def lock_and_read_data_version(db):
    """发布证据前以 FOR SHARE 锁住版本行，与更正事务的 UPDATE 互斥，关闭检查与提交间的竞态窗口。"""
    row=db.execute(text("SELECT value FROM meta WHERE key='data_version' FOR SHARE")).scalar()
    return int(row) if row is not None else 1

def racing_imports_overlap(db,since_version,read_ids):
    """数据版本 since_version 之后生效的更正，是否触及给定读集。"""
    if read_ids is None:return False
    read=set(read_ids)
    rows=db.scalars(select(Import.affected_invoice_ids).where(Import.status=='applied',Import.data_version>since_version))
    return any(read & set(ids or []) for ids in rows)

def parse_bool(raw,lineno):
    v=(raw or '').strip().lower()
    if v in VOID_TRUE:return True
    if v in VOID_FALSE:return False
    raise ValueError(f'第{lineno}行 void 标记无法识别：{raw}')

def parse_rows(content):
    try:reader=csv.DictReader(io.StringIO(content))
    except Exception as exc:raise ValueError('CSV 无法解析：'+str(exc)[:120])
    names=reader.fieldnames or []
    missing=[f for f in FIELDS if f not in names]
    if missing:raise ValueError('缺少必需列：'+','.join(missing))
    rows=[]
    for lineno,raw in enumerate(reader,start=2):
        try:
            invoice_id=int((raw['invoice_id'] or '').strip())
            version=int((raw['version'] or '').strip())
            if version<1:raise InvalidOperation()
            amount=Decimal((raw['amount'] or '').strip())
            temperature=Decimal((raw['temperature'] or '').strip())
        except (InvalidOperation,ValueError,TypeError):
            raise ValueError(f'第{lineno}行数字字段格式错误（账单号/版本/金额/温度）')
        customer=(raw['customer'] or '').strip();route=(raw['route'] or '').strip()
        status=(raw['status'] or '').strip()
        if not customer or not route:raise ValueError(f'第{lineno}行客户或路线为空')
        if status not in ('settled','pending'):raise ValueError(f'第{lineno}行 status 仅允许 settled/pending')
        rows.append({'line_no':lineno,'invoice_id':invoice_id,'version':version,'customer':customer,'route':route,
                     'amount':amount,'temperature':temperature,'status':status,'void':parse_bool(raw['void'],lineno)})
    if not rows:raise ValueError('文件没有数据行')
    if len(rows)>MAX_ROWS:raise ValueError(f'单次最多导入 {MAX_ROWS} 行')
    return rows

def canonical(row):
    return {'customer':row['customer'],'route':row['route'],'amount':str(row['amount']),'temperature':str(row['temperature']),'status':row['status'],'void':row['void']}

def same_content(row,inv):
    return (inv.customer==row['customer'] and inv.route==row['route'] and Decimal(inv.amount)==row['amount']
            and Decimal(inv.temperature)==row['temperature'] and inv.status==row['status'] and inv.voided==row['void'])

def decide(row,inv):
    """返回 (disposition, note)；reject 表示整批拒绝。"""
    if inv is None:
        return 'applied',('新账单补入并作废' if row['void'] else '新账单补入')
    if inv.voided:
        if inv.version==row['version'] and same_content(row,inv):return 'skipped_duplicate','作废记录重放，保持原状'
        if inv.version==row['version']:return 'reject',f'账单 {inv.invoice_id} 版本 {row["version"]} 与已作废记录内容冲突'
        return 'skipped_void',f'账单已于 v{inv.version} 作废，作废为终态不再复活'
    if row['version']>inv.version:return 'applied',f'采用新版本 v{row["version"]}（当前 v{inv.version}）'
    if row['version']<inv.version:return 'skipped_stale',f'已有更新版本 v{inv.version}，忽略旧版本 v{row["version"]}'
    if same_content(row,inv):return 'skipped_duplicate','同版本同内容，幂等跳过'
    return 'reject',f'账单 {inv.invoice_id} 版本 {row["version"]} 内容冲突'

def apply_row(inv,row):
    inv.customer=row['customer'];inv.route=row['route'];inv.amount=row['amount']
    inv.temperature=row['temperature'];inv.status=row['status'];inv.voided=row['void'];inv.version=row['version']

def check_file_duplicates(rows):
    seen={}
    for r in rows:
        key=(r['invoice_id'],r['version']);canon=canonical(r)
        if key in seen and seen[key]!=canon:
            raise ValueError(f'文件内账单 {key[0]} 版本 {key[1]} 出现两种内容，整批拒绝')
        seen.setdefault(key,canon)

def dedupe_rows(rows):
    seen=set();out=[]
    for r in rows:
        key=(r['invoice_id'],r['version'])
        if key in seen:continue
        seen.add(key);out.append(r)
    return out

def serialize_import(x,replayed=False):
    return {'id':x.id,'file_name':x.file_name,'status':x.status,'total_rows':x.total_rows,'applied_rows':x.applied_rows,
            'skipped_rows':x.skipped_rows,'reject_reason':x.reject_reason,'affected_invoice_ids':x.affected_invoice_ids or [],
            'data_version':x.data_version,'actor':x.actor,'created_at':str(x.created_at),'replayed':replayed}

def record_rejection(db,file_name,content_hash,actor,total,reason,rows=None):
    rec=Import(file_name=file_name,content_hash=content_hash,status='rejected',total_rows=total,
               applied_rows=0,skipped_rows=0,reject_reason=reason,affected_invoice_ids=[],actor=actor)
    db.add(rec)
    try:
        db.flush()
        for r in rows or []:
            db.add(ImportItem(import_id=rec.id,line_no=r['line_no'],invoice_id=r['invoice_id'],version=r['version'],
                              void=r['void'],content=canonical(r),disposition='rejected',note=reason[:300]))
        db.commit()
    except IntegrityError:
        db.rollback()
        existing=db.scalar(select(Import).where(Import.content_hash==content_hash))
        if existing:return existing
        raise
    db.refresh(rec)
    return rec

def import_corrections(db,data,actor):
    content_hash=hashlib.sha256(data.content.encode()).hexdigest()
    existing=db.scalar(select(Import).where(Import.content_hash==content_hash))
    if existing:return serialize_import(existing,replayed=True)
    try:
        rows=parse_rows(data.content)
        check_file_duplicates(rows)
    except ValueError as exc:
        return serialize_import(record_rejection(db,data.file_name,content_hash,actor,0,str(exc)))
    rows=dedupe_rows(rows)
    rec=Import(file_name=data.file_name,content_hash=content_hash,status='applied',total_rows=len(rows),actor=actor)
    items=[];affected=set();applied=skipped=0
    try:
        ensure_meta(db)
        for iid in sorted({r['invoice_id'] for r in rows}):
            db.execute(text('SELECT pg_advisory_xact_lock(:k)'),{'k':iid})
        current={inv.invoice_id:inv for inv in db.scalars(select(Invoice).where(Invoice.invoice_id.in_(sorted({r['invoice_id'] for r in rows}))))}
        for r in sorted(rows,key=lambda r:(r['invoice_id'],r['version'],r['line_no'])):
            inv=current.get(r['invoice_id'])
            disposition,note=decide(r,inv)
            if disposition=='reject':raise RejectBatch(note)
            if disposition=='applied':
                if inv is None:
                    inv=Invoice(invoice_id=r['invoice_id'],customer='',route='',amount=0,temperature=0,status='pending',version=0,voided=False)
                    apply_row(inv,r);db.add(inv);current[r['invoice_id']]=inv
                else:apply_row(inv,r)
                affected.add(r['invoice_id']);applied+=1
            else:skipped+=1
            items.append(ImportItem(line_no=r['line_no'],invoice_id=r['invoice_id'],version=r['version'],
                                    void=r['void'],content=canonical(r),disposition=disposition,note=note[:300]))
        new_version=int(db.execute(text("UPDATE meta SET value=(value::int+1)::text WHERE key='data_version' RETURNING value")).scalar())
        rec.data_version=new_version;rec.affected_invoice_ids=sorted(affected)
        rec.applied_rows=applied;rec.skipped_rows=skipped
        db.add(rec)
        try:
            db.flush()  # content_hash 唯一约束在此兜底相同文件的并发导入
            for item in items:item.import_id=rec.id
            db.add_all(items)
            db.commit()
        except IntegrityError:
            db.rollback()
            existing=db.scalar(select(Import).where(Import.content_hash==content_hash))
            if existing:return serialize_import(existing,replayed=True)
            raise
        db.refresh(rec)
        return serialize_import(rec)
    except RejectBatch as exc:
        db.rollback()
        return serialize_import(record_rejection(db,data.file_name,content_hash,actor,len(rows),str(exc),rows))

def list_imports(db):
    return [serialize_import(x) for x in db.scalars(select(Import).order_by(Import.id.desc()))]

def get_import(db,import_id):
    imp=db.get(Import,import_id)
    if not imp:
        from fastapi import HTTPException
        raise HTTPException(404,'导入记录不存在')
    items=db.scalars(select(ImportItem).where(ImportItem.import_id==import_id).order_by(ImportItem.line_no))
    return {**serialize_import(imp),'items':[{'line_no':i.line_no,'invoice_id':i.invoice_id,'version':i.version,'void':i.void,
            'content':i.content,'disposition':i.disposition,'note':i.note} for i in items]}
