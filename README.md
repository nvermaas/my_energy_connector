# my_energy_connector

* fastapi web service connecting to a mongodb database containing my energy data.
* a commandline tool to create and update the mongodb database

## API example
http://middle-earth:8015/my_energy/api2/getseries/?start=2024-06-06&end=2024-06-07&interval=Hour


### build and deploy 

The following manual steps should not be needed because of the github action that automatically deploys.
But in case manual build and deploy is needed, this is the procedure.

```commandline
> cd ~/my_docker/my_energy_connector
> git pull
> docker build -t my_energy_connector:latest .

> cd ~/shared
> docker-compose  -p energy -f ./docker-compose-energy.yml up -d

or

> docker run -d --name my-energy-connector --mount type=bind,source=$HOME/shared,target=/shared -p 8015:8015 --restart always my-energy-connector:latest

```

### operations

Access the Docker container containing the script
```commandline
> docker exec -it my_energy_connector sh
# python my-energy-connector.py
```


### scp-sqlite
scp the `my_energy.sqlite3` database from its production environment on a raspberry pi.
This takes about 10 seconds.

````commandline
# python my-energy-connector.py --command scp-sqlite --remote_sqlite_database pi:<password>@192.168.178.64::/home/pi/my_energy/my_energy.sqlite3 --sqlite_database /shared/my_energy.sqlite3
````

### sqlite-to-mongo
Convert the existing sqlite database to mongodb. 
This clears the entire mongo database, but it only takes about 10 seconds for the whole operation and can safely be repeated

The mongodb is identified by the environment variable `DATABASE_URL`.
If omitted the default will be DATABASE_URL='mongodb://mongodb:27017/'

````commandline
# python my-energy-connector.py --command sqlite-to-mongo --sqlite_database /shared/my_energy.sqlite3
````

### update-to-now
Update the mongodb to the current timestamp with records from the my_energy.sqlite3 database, if available.

````commandline
# python my-energy-connector.py --command update-to-now --sqlite_database /shared/my_energy.sqlite3
````
### scp-update-to-now
The 2 previous commands combined, copying the latest sqlite database and update the mongodb to the current timestamp

````commandline
# python my-energy-connector.py --command scp-update-to-now --remote_sqlite_database pi:<password>>@192.168.178.64::/home/pi/my_energy/my_energy.sqlite3 --sqlite_database /shared/my_energy.sqlite3
````

### run-server
Run the fastapi webserver, after which the 'getseries' command can be used to retrieve data in a json response.
This command is automatically started when the container is spun up, so the webserver is available.
````commandline
# python --command runserver
````

### getseries
Example of a call to the api
````commandline
# python my-energy-connector.py --command getseries --start 2024-06-21 --end 2024-06-22 --interval Hour
# python my-energy-connector.py --command getseries --start 2024-07-19 --end 2024-07-20 --interval Hour
````