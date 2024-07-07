from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from database.db import DB

app = FastAPI(default_response_class=ORJSONResponse,port=8000)

# http://localhost:8000/getseries/?start=2024-06-21&end=2024-06-22&interval=Hour
@app.get('/{getseries}/')
def getseries(start, end, interval):
    return DB.get_series(start,end,interval,DB.collection)