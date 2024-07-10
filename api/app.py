try:
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
        allow_origins=["localhost:3000","192.168.178.128","uilennest.net","middle-earth"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # http://localhost:8000/getseries/?start=2024-06-21&end=2024-06-22&interval=Hour
    @app.get('/my_energy/api2/{getseries}/')
    def getseries(start, end, interval):
        return DB.get_series(start,end,interval)[0]

except:
    print('no fast api')