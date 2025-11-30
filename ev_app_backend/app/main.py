from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import settings
from app.api.endpoints import auth, stations, vehicles

app = FastAPI(
    title="EV-APP Backend",
    description="Electric Vehicle Management and Analytics Platform",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_HOSTS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth.router, prefix="/api/auth", tags=["authentication"])
app.include_router(stations.router, prefix="/api/stations", tags=["stations"])
app.include_router(vehicles.router, prefix="/api/vehicles", tags=["vehicles"])

@app.get("/")
async def root():
    return {
        "message": "EV-APP Backend API", 
        "version": "1.0.0",
        "status": "running"
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.get("/database-info")
async def database_info():
    return {
        "tables": {
            "charging_stations": "242,417 rows",
            "stations": "48,000 rows", 
            "ev_models": "63 rows",
            "users": "1 row",
            "country_summary": "122 rows",
            "world_summary": "122 rows"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)