from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from app.core.database import get_db
from app.models.vehicle import EVModel
from app.schemas.vehicle import VehicleResponse

router = APIRouter()

@router.get("/", response_model=List[VehicleResponse])
async def get_vehicles(
    skip: int = 0,
    limit: int = 100,
    manufacturer: str = None,
    db: Session = Depends(get_db)
):
    query = db.query(EVModel)
    
    if manufacturer:
        query = query.filter(EVModel.manufacturer.ilike(f"%{manufacturer}%"))
    
    vehicles = query.offset(skip).limit(limit).all()
    return vehicles

@router.get("/{vehicle_id}", response_model=VehicleResponse)
async def get_vehicle(vehicle_id: int, db: Session = Depends(get_db)):
    vehicle = db.query(EVModel).filter(EVModel.id == vehicle_id).first()
    if not vehicle:
        raise HTTPException(status_code=404, detail="Vehicle not found")
    return vehicle

@router.get("/stats/summary")
async def get_vehicle_stats(db: Session = Depends(get_db)):
    total_vehicles = db.query(EVModel).count()
    manufacturers = db.query(EVModel.manufacturer).distinct().count()
    
    return {
        "total_vehicles": total_vehicles,
        "manufacturers": manufacturers,
        "message": f"Found {total_vehicles} EV models from {manufacturers} manufacturers"
    }