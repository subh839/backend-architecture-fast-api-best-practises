from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class VehicleBase(BaseModel):
    model_name: Optional[str] = None
    manufacturer: Optional[str] = None
    battery_capacity_kwh: Optional[float] = None
    range_km: Optional[float] = None
    charging_power_kw: Optional[float] = None
    fast_charging_support: Optional[bool] = None
    release_year: Optional[int] = None
    vehicle_type: Optional[str] = None
    price_usd: Optional[float] = None

class VehicleResponse(VehicleBase):
    id: int
    created_at: datetime

    class Config:
        orm_mode = True