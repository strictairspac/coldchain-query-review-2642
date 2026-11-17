from sqlalchemy.orm import Mapped,mapped_column
from sqlalchemy import String,Integer,Numeric,Boolean
from decimal import Decimal
from ..config.database import Base
class Invoice(Base):
    __tablename__='invoices'
    invoice_id:Mapped[int]=mapped_column(Integer,primary_key=True)
    customer:Mapped[str]=mapped_column(String(60))
    route:Mapped[str]=mapped_column(String(60))
    amount:Mapped[Decimal]=mapped_column(Numeric(12,2))
    temperature:Mapped[Decimal]=mapped_column(Numeric(4,1))
    status:Mapped[str]=mapped_column(String(20))
    version:Mapped[int]=mapped_column(Integer,server_default='1')
    voided:Mapped[bool]=mapped_column(Boolean,server_default='false')
