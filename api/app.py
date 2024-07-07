from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from database.energy_db import DB

app = FastAPI(
    title='my_energy_connector',
    version="0.9.0",
    description="backend API for mongo my_energy database",
    default_response_class=ORJSONResponse,
    port=8000)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# http://localhost:8000/getseries/?start=2024-06-21&end=2024-06-22&interval=Hour
@app.get('/{getseries}/')
def getseries(start, end, interval):
    return DB.get_series(start,end,interval)