from sqlalchemy import Column, Integer, String, Float, DateTime, Text
from sqlalchemy.sql import func
from app.core.database import Base

class Station(Base):
    __tablename__ = "stations"

    id = Column(Integer, primary_key=True, index=True)
    station_id = Column(String, unique=True, index=True)
    name = Column(String)
    lat = Column(Float)
    lon = Column(Float)
    address = Column(Text)
    city = Column(String)
    country = Column(String)
    power_class_kw = Column(Float)
    connectors = Column(String)
    created_at = Column(DateTime(timezone=True), server_default=func.now())