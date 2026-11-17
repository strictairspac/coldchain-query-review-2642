from sqlalchemy.orm import Mapped,mapped_column
from sqlalchemy import ForeignKey,JSON,Integer
from ..config.database import Base
class Evidence(Base):
    __tablename__='evidence'
    id:Mapped[int]=mapped_column(primary_key=True)
    batch_id:Mapped[int]=mapped_column(ForeignKey('batches.id'),unique=True)
    result:Mapped[dict]=mapped_column(JSON)
    data_version:Mapped[int|None]=mapped_column(Integer,nullable=True)
    read_invoice_ids:Mapped[list|None]=mapped_column(JSON,nullable=True)
