import os
from pymongo import MongoClient
import sqlite3
import paramiko
from datetime import datetime,timedelta

class EnergyDB:
    def __init__(self):
        try:
            print(os.environ['DATABASE_URL'])
            self.mongo_host = os.environ['DATABASE_URL']
        except:
            self.mongo_host = "mongodb://middle-earth:27017/"
        print('host: '+ self.mongo_host)

        self.collection = MongoClient(self.mongo_host)["my_energy"]["energy_records"]

    def convert_rows(self, rows):
        energy_records = []
        previous_row = None
        count_holes = 0
        next_expected_timestamp = None
        for row in rows:

            if not previous_row:
                previous_row = row

            timestamp = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')

            if next_expected_timestamp:
                if timestamp != next_expected_timestamp:
                    delta_timestamp = timestamp - next_expected_timestamp + timedelta(minutes=5)
                    print(
                        f'unexpected timestamp: expected = {next_expected_timestamp}, found = {timestamp}, gap size = {delta_timestamp}')
                    count_holes = count_holes + 1

            delta_kwh_181 = row[2] - previous_row[2]
            delta_kwh_182 = row[3] - previous_row[3]
            delta_kwh_281 = row[4] - previous_row[4]
            delta_kwh_282 = row[5] - previous_row[5]

            delta_netlow = delta_kwh_181 - delta_kwh_281
            delta_nethigh = delta_kwh_182 - delta_kwh_282
            delta_generation = delta_kwh_281 + delta_kwh_282
            delta_gas = row[1] - previous_row[1]

            growatt_power = row[13]

            if (growatt_power and growatt_power > 0):
                #growatt_power = growatt_power / 12  # seems OK, total power is ok
                delta_consumption = delta_netlow + delta_nethigh + growatt_power  # seems OK
            else:
                # old values, before the solar panel data was in the database
                delta_consumption = delta_kwh_181 + delta_kwh_182

            energy_record = {
                "timestamp": timestamp,
                "gas": row[1],
                "kwh_181": row[2],
                "kwh_182": row[3],
                "kwh_281": row[4],
                "kwh_282": row[5],
                "growatt_power": growatt_power,
                "growatt_power_today": row[14],
                "delta_netlow": delta_netlow,
                "delta_nethigh": delta_nethigh,
                "delta_generation": delta_generation,
                "delta_consumption": delta_consumption,
                "delta_gas": delta_gas
            }

            energy_records.append(energy_record)
            previous_row = row
            next_expected_timestamp = timestamp + timedelta(minutes=5)

        return energy_records,count_holes


    def scp_sqlite(self,remote_sqlite_database,local_sqlite_database):
        """
        scp/ftp a remote sqlite_database to a local file
        """
        def parse_connect_string(url):
            """
            parse a connect string like: pi:my_password@raspiqbox:/home/pi/my_energy/my_energy.sqlite3
            """
            left = url.split('@')[0]
            user = left.split(':')[0]
            password = left.split(':')[1]

            right = url.split('@')[1]
            host = right.split("::")[0]
            remote_path = right.split("::")[1]

            return user, password, host, remote_path

        print('scp_sqlite')
        print(f'remote  : {remote_sqlite_database}')
        print(f'local : {local_sqlite_database}')
        print('--------------------------')
        print('copying...')
        username, password, hostname, remote_path = parse_connect_string(remote_sqlite_database)

        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=hostname, port=22, username=username, password=password,
                           allow_agent=False, look_for_keys=False)

        ftp_client = ssh_client.open_sftp()
        ftp_client.get(remote_path, local_sqlite_database)
        ftp_client.close()
        print('done.')

    def convert_from_sqlite_to_mongo(self, sqlite_database):
        """
        Convert the full my_energy.sqlite database to mongodb.
        This drops the existing collection in mongodb.
        The whole operation takes about 30 seconds.
        """

        print('convert_from_sqlite_to_mongo')
        print(f'sqlite : {sqlite_database}')
        print(f'mongo  : {self.mongo_host}')
        print('--------------------------')

        # connect to sqlite database (file)
        conn = sqlite3.connect(sqlite_database)
        cur = conn.cursor()

        # execute query
        print(f'getting records from {sqlite_database}...')
        cur.execute('select * from my_energy_server_energyrecord order by timestamp')
        rows = cur.fetchall()
        print(f'{len(rows)} records read, creating json records...')

        energy_records, holes = self.convert_rows(rows)
        print(f'{holes} missing 5-min timestamps')

        print(f"inserting records into {self.mongo_host}...")
        self.collection.drop()
        result = self.collection.insert_many(energy_records)
        print(f'{len(result.inserted_ids)} records inserted.')

        # close sqlite database
        conn.close()

    def update_to_now(self, sqlite_database):
        """
        Update the records from the my_energy (sqlite) database into the mongodb database.
        """

        print('update_to_now')
        print(f'sqlite : {sqlite_database}')
        print(f'mongo  : {self.mongo_host}')
        print('--------------------------')

        # get the latest timestamp from the mongodb database
        # Query the collection to find the latest timestamp
        latest_document = self.collection.find().sort('timestamp', -1).limit(1)

        # Extract the timestamp
        latest_timestamp = None
        for doc in latest_document:
            latest_timestamp = doc['timestamp']

        print("Latest Timestamp:", latest_timestamp)

        # connect to sqlite database (file)
        conn = sqlite3.connect(sqlite_database)
        cur = conn.cursor()

        # execute query
        print(f'getting records from {sqlite_database}...')
        query = f"select * from my_energy_server_energyrecord where timestamp > '{latest_timestamp}' order by timestamp"
        cur.execute(query)
        rows = cur.fetchall()
        print(f'{len(rows)} new records read, creating json records...')

        # loop through the rows and convert them to energy_records in json for storage in the mongo database
        energy_records,holes = self.convert_rows(rows)

        print(f'{holes} missing 5-min timestamps')
        print(f"inserting records into {self.mongo_host}...")

        result = self.collection.insert_many(energy_records)
        print(f'{len(result.inserted_ids)} new records inserted.')

        # close sqlite database
        conn.close()


    def calculate_intervals(self, start, end, interval_type):
        """
        Calculate the number of intervals between start and end timestamps based on interval type.

        Parameters:
        start (datetime): Start timestamp.
        end (datetime): End timestamp.
        interval_type (str): Type of interval ('HOUR', 'DAY', 'MONTH', 'YEAR').

        Returns:
        int: Number of intervals.
        """
        total_intervals = 0
        if interval_type == 'HOUR':
            total_intervals = (end - start).total_seconds() // 3600
        elif interval_type == 'DAY':
            total_intervals = (end - start).days
        elif interval_type == 'MONTH':
            total_intervals = (end.year - start.year) * 12 + (end.month - start.month)
        elif interval_type == 'YEAR':
            total_intervals = end.year - start.year

        return total_intervals


    def get_series(self, start, end, interval):
        # reconstruction this data:
        # http://192.168.178.64:81/my_energy/api/getseries?from=2024-06-21&to=2024-06-22&resolution=Hour

        print(f'get_series(from {start} to {end} per {interval})')
        timestamp_start = datetime.strptime(start, '%Y-%m-%d')
        timestamp_end = datetime.strptime(end, '%Y-%m-%d')

        interval_operation = '$hour'
        if interval.upper() == 'HOUR':
            interval_operation = '$hour'
        elif interval.upper() == 'DAY':
            interval_operation = '$dayOfMonth'
        elif interval.upper() == 'MONTH':
            interval_operation = '$month'
        elif interval.upper() == 'YEAR':
            interval_operation = '$year'

        # Query the collection for documents within the specified time range
        # Aggregation pipeline
        pipeline = [
            {
                '$match': {
                    'timestamp': {
                        '$gte': timestamp_start,
                        '$lt': timestamp_end
                    }
                }
            }, {
                '$project': {
                    'interval': {
                        interval_operation: '$timestamp'
                    },
                    'delta_netlow': 1,
                    'delta_consumption': 1,
                    'delta_nethigh': 1,
                    'delta_gas': 1,
                    'delta_generation': 1,
                    'growatt_power': 1,
                    'growatt_power_today': 1,
                }
            }, {
                '$group': {
                    '_id': {
                        'interval': '$interval'
                    },
                    'netlow': {
                        '$sum': '$delta_netlow'
                    },
                    'consumption': {
                        '$sum': '$delta_consumption'
                    },
                    'nethigh': {
                        '$sum': '$delta_nethigh'
                    },
                    'gas': {
                        '$sum': '$delta_gas'
                    },
                    'generation': {
                        '$sum': '$delta_generation'
                    },
                    'summed_growatt_power': {
                        '$sum': '$growatt_power'
                    },
                },

            },{
                '$addFields': {
                    'growatt_power': {
                        '$divide': ['$summed_growatt_power', 12]
                    }
                }
            },{
                '$sort': {
                    '_id.interval': 1
                }
            },
            {
                '$group': {
                    '_id': None,
                    'netlow': {
                        '$push': '$netlow'
                    },
                    'consumption': {
                        '$push': '$consumption'
                    },
                    'nethigh': {
                        '$push': '$nethigh'
                    },
                    'gas': {
                        '$push': '$gas'
                    },
                    'generation': {
                        '$push': '$generation'
                    },
                    'growatt_power': {
                        '$push': '$growatt_power'
                    },
                    'total_netlow': {'$sum': '$netlow'},
                    'total_consumption': {'$sum': '$consumption'},
                    'total_nethigh': {'$sum': '$nethigh'},
                    'total_gas': {'$sum': '$gas'},
                    'total_generation': {'$sum': '$generation'},
                    'total_growatt_power': {'$sum': '$growatt_power'}
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'data': [
                        {
                            'data': '$netlow',
                            'total': '$total_netlow',
                            'type': 'NetLow',
                            'energyType': 'NetLow'
                        },
                        {
                            'data': '$consumption',
                            'total': '$total_consumption',
                            'type': 'Consumption',
                            'energyType': 'Consumption'
                        },
                        {
                            'data': '$nethigh',
                            'total': '$total_nethigh',
                            'type': 'NetHigh',
                            'energyType': 'NetHigh'
                        },
                        {
                            'data': '$gas',
                            'total': '$total_gas',
                            'type': 'Gas',
                            'energyType': 'Gas'
                        },
                        {
                            'data': '$generation',
                            'total': '$total_generation',
                            'type': 'Generation',
                            'energyType': 'Generation'
                        },
                        {
                            'data': [0,0,0],
                            'total': '0',
                            'type': 'Temperature',
                            'energyType': 'Temperature'
                        },
                        {
                            'data': [0, 0, 0],
                            'total': '0',
                            'type': 'Rain',
                            'energyType': 'Rain'
                        },
                        {
                            'data': [0, 0, 0],
                            'total': '0',
                            'type': 'Wind Speed',
                            'energyType': 'Wind Speed'
                        },
                        {
                            'data': [0, 0, 0],
                            'total': '0',
                            'type': 'Wind Gust',
                            'energyType': 'Wind Gust'
                        },
                        {
                            'data': [0, 0, 0],
                            'total': '0',
                            'type': 'Wind Direction',
                            'energyType': 'Wind Direction'
                        },
                        {
                            'data': '$growatt_power',
                            'total': '$total_growatt_power',
                            'type': 'Solar Panels'
                        }
                    ]
                }
            }
        ]
        # Run the aggregation query
        results = list(self.collection.aggregate(pipeline))

        # Calculate the number of items, can be used to pad lists with 0's
        expected_nr_of_items = int(self.calculate_intervals(timestamp_start, timestamp_end, interval.upper()))
        print(f"Interval: {interval}, Number of items: {expected_nr_of_items}")

        # pad missing data items with 0 values
        for result in results:
            for r in result['data']:
                data = r['data']
                found_nr_of_items = len(data)

                for i in range(expected_nr_of_items-found_nr_of_items):
                    data.append(0)

        return results


def get_mongodb_collection(args):
    # connect to mongodb
    return MongoClient(args.target_mongo)[args.database][args.collection]

# instantiate the database
DB = EnergyDB()