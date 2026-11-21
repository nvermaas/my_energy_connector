try:
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import ORJSONResponse
    from database.energy_db import DB

    app = FastAPI()

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # http://localhost:8000/getseries/?start=2024-06-21&end=2024-06-22&interval=Hour
    @app.get('/my_energy/api2/{getseries}/')
    def getseries(start, end, interval):
        return DB.get_series(start,end,interval)[0]

except Exception as e:
    print(e)