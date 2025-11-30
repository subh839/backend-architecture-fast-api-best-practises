from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.sql import func
from app.core.database import Base

class EVModel(Base):
    __tablename__ = "ev_models"

    id = Column(Integer, primary_key=True, index=True)
    model_name = Column(String)
    manufacturer = Column(String)
    battery_capacity_kwh = Column(Float)
    range_km = Column(Float)
    charging_power_kw = Column(Float)
    release_year = Column(Integer)
    created_at = Column(DateTime(timezone=True), server_default=func.now())