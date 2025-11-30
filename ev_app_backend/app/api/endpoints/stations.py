from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from app.core.database import get_db
from app.schemas.station import StationResponse, StationSearch
from app.models.station import Station
from app.services.station_service import station_service

router = APIRouter()

@router.get("/", response_model=List[StationResponse])
async def get_stations(
    skip: int = 0,
    limit: int = 100,
    country: Optional[str] = None,
    db: Session = Depends(get_db)
):
    query = db.query(Station)
    
    if country:
        query = query.filter(Station.country == country)
    
    stations = query.offset(skip).limit(limit).all()
    return stations

@router.get("/nearby", response_model=List[StationResponse])
async def get_nearby_stations(
    lat: float = Query(..., description="Latitude"),
    lng: float = Query(..., description="Longitude"),
    radius: float = Query(10.0, description="Radius in kilometers"),
    min_power: Optional[float] = Query(None, description="Minimum power in kW"),
    db: Session = Depends(get_db)
):
    stations = station_service.get_nearby_stations(db, lat, lng, radius, min_power)
    return stations

@router.get("/stats")
async def get_station_stats(db: Session = Depends(get_db)):
    total_stations = db.query(Station).count()
    stations_with_coords = db.query(Station).filter(
        Station.lat.isnot(None),
        Station.lon.isnot(None)
    ).count()
    
    # Get country distribution
    country_stats = db.query(
        Station.country, 
        Station.power_class_kw
    ).filter(
        Station.country.isnot(None)
    ).all()
    
    countries = len(set([stat[0] for stat in country_stats if stat[0]]))
    avg_power = sum([stat[1] for stat in country_stats if stat[1]]) / len([stat[1] for stat in country_stats if stat[1]])
    
    return {
        "total_stations": total_stations,
        "stations_with_coordinates": stations_with_coords,
        "countries_covered": countries,
        "average_power_kw": round(avg_power, 2)
    }

@router.get("/{station_id}", response_model=StationResponse)
async def get_station(station_id: int, db: Session = Depends(get_db)):
    station = db.query(Station).filter(Station.id == station_id).first()
    if not station:
        raise HTTPException(status_code=404, detail="Station not found")
    return station