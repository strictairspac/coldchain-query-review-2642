from sqlalchemy.orm import Mapped,mapped_column
from sqlalchemy import String,Text,Integer,Boolean,ForeignKey,JSON,DateTime,func
from datetime import datetime
from ..config.database import Base
class Import(Base):
    __tablename__='imports'
    id:Mapped[int]=mapped_column(primary_key=True)
    file_name:Mapped[str]=mapped_column(String(200))
    content_hash:Mapped[str]=mapped_column(String(64),unique=True)
    status:Mapped[str]=mapped_column(String(20))
    total_rows:Mapped[int]=mapped_column(Integer,default=0)
    applied_rows:Mapped[int]=mapped_column(Integer,default=0)
    skipped_rows:Mapped[int]=mapped_column(Integer,default=0)
    reject_reason:Mapped[str|None]=mapped_column(Text,nullable=True)
    affected_invoice_ids:Mapped[list]=mapped_column(JSON,default=list)
    data_version:Mapped[int|None]=mapped_column(Integer,nullable=True)
    actor:Mapped[str]=mapped_column(String(120))
    created_at:Mapped[datetime]=mapped_column(DateTime,server_default=func.now())
class ImportItem(Base):
    __tablename__='import_items'
    id:Mapped[int]=mapped_column(primary_key=True)
    import_id:Mapped[int]=mapped_column(ForeignKey('imports.id'))
    line_no:Mapped[int]=mapped_column(Integer)
    invoice_id:Mapped[int]=mapped_column(Integer)
    version:Mapped[int]=mapped_column(Integer)
    void:Mapped[bool]=mapped_column(Boolean)
    content:Mapped[dict]=mapped_column(JSON)
    disposition:Mapped[str]=mapped_column(String(20))
    note:Mapped[str|None]=mapped_column(String(300),nullable=True)
