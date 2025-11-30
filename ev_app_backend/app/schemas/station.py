from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class StationBase(BaseModel):
    station_id: Optional[str] = None
    name: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    address: Optional[str] = None
    city: Optional[str] = None
    country: Optional[str] = None
    power_class_kw: Optional[float] = None
    connectors: Optional[str] = None

class StationResponse(StationBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True

class StationSearch(BaseModel):
    lat: float
    lng: float
    radius: float = 10.0
    min_power: Optional[float] = None