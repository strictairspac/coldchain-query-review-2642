from sqlalchemy.orm import Mapped,mapped_column
from sqlalchemy import String
from ..config.database import Base
class Meta(Base):
    __tablename__='meta'
    key:Mapped[str]=mapped_column(String(60),primary_key=True)
    value:Mapped[str]=mapped_column(String(200))
