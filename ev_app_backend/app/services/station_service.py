from sqlalchemy.orm import Session
from typing import List, Optional
import math
from app.models.station import Station

class StationService:
    def get_nearby_stations(
        self, 
        db: Session, 
        lat: float, 
        lng: float, 
        radius: float = 10.0,
        min_power: Optional[float] = None
    ) -> List[Station]:
        """Get stations within radius of given coordinates"""
        try:
            query = db.query(Station).filter(
                Station.lat.isnot(None),
                Station.lon.isnot(None)
            )
            
            if min_power:
                query = query.filter(Station.power_class_kw >= min_power)
            
            stations = query.all()
            
            # Filter by distance
            nearby_stations = []
            for station in stations:
                distance = self._calculate_distance(lat, lng, station.lat, station.lon)
                if distance <= radius:
                    station.distance = distance
                    nearby_stations.append(station)
            
            # Sort by distance
            nearby_stations.sort(key=lambda x: x.distance)
            
            return nearby_stations
            
        except Exception as e:
            print(f"Error getting nearby stations: {e}")
            return []
    
    def _calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two coordinates in kilometers"""
        R = 6371  # Earth radius in kilometers
        
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        
        a = (math.sin(dlat/2) * math.sin(dlat/2) +
             math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
             math.sin(dlon/2) * math.sin(dlon/2))
        
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        distance = R * c
        
        return distance

station_service = StationService()