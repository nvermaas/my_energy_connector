import os,sys,argparse
import sqlite3

from pymongo import MongoClient
from datetime import datetime,timedelta,timezone

def get_mongodb_collection(args):
    # connect to mongodb
    client = MongoClient(args.target)
    db = client[args.database]
    collection = db[args.collection]
    return collection


def convert_from_sqlite_to_mongo_basic(args):
    print('convert_from_sqlite_to_mongo')
    print(f'source : {args.source}')
    print(f'target : {args.target}')
    print('--------------------------')

    # connect to sqlite database (file)
    conn = sqlite3.connect(args.source)
    cur = conn.cursor()

    # connect to mongodb
    collection = get_mongodb_collection(args)

    # execute query
    print(f'getting records from {args.source}...')
    cur.execute('select * from my_energy_server_energyrecord')
    rows = cur.fetchall()
    print(f'{len(rows)} records read, creating json records...')

    energy_records = []
    for row in rows:
        timestamp = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')

        energy_record = {
            "timestamp" : timestamp,
            "gas"       : row[1],
            "kwh_181"   : row[2],
            "kwh_182"   : row[3],
            "kwh_281"   : row[4],
            "kwh_282"   : row[5],
            "growatt_power"       : row[13],
            "growatt_power_today" : row[14]
        }

        energy_records.append(energy_record)

    print(f"inserting records into {args.target}...")
    collection.drop()
    result = collection.insert_many(energy_records)
    print(f'{len(result.inserted_ids)} records inserted.')

    # close sqlite database
    conn.close()

def convert_from_sqlite_to_mongo(args):
    print('convert_from_sqlite_to_mongo')
    print(f'source : {args.source}')
    print(f'target : {args.target}')
    print('--------------------------')

    # connect to sqlite database (file)
    conn = sqlite3.connect(args.source)
    cur = conn.cursor()

    # connect to mongodb
    collection = get_mongodb_collection(args)

    # execute query
    print(f'getting records from {args.source}...')
    cur.execute('select * from my_energy_server_energyrecord order by timestamp')
    rows = cur.fetchall()
    print(f'{len(rows)} records read, creating json records...')

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
                print(f'unexpected timestamp: expected = {next_expected_timestamp}, found = {timestamp}, gap size = {delta_timestamp}')
                count_holes = count_holes + 1

        delta_kwh_181 = row[2] - previous_row[2]
        delta_kwh_182 = row[3] - previous_row[3]
        delta_kwh_281 = row[4] - previous_row[4]
        delta_kwh_282 = row[5] - previous_row[5]

        delta_netlow = delta_kwh_181 - delta_kwh_281     # 8 verschil
        delta_nethigh = delta_kwh_182 - delta_kwh_282    # OK
        delta_generation = delta_kwh_281 + delta_kwh_282 # OK
        delta_gas = row[1] - previous_row[1]             # OK

        growatt_power = row[13]

        if (growatt_power and growatt_power>0):
            growatt_power = growatt_power / 12                               # seems OK, total power is ok
            delta_consumption = delta_netlow + delta_nethigh + growatt_power # seems OK
        else:
            # old values, before the solar panel data was in the database
            delta_consumption = delta_kwh_181 + delta_kwh_182

        energy_record = {
            "timestamp" : timestamp,
            "gas"       : row[1],
            "kwh_181"   : row[2],
            "kwh_182"   : row[3],
            "kwh_281"   : row[4],
            "kwh_282"   : row[5],
            "growatt_power"       : growatt_power,
            "growatt_power_today" : row[14],
            "delta_netlow" : delta_netlow,
            "delta_nethigh": delta_nethigh,
            "delta_generation": delta_generation,
            "delta_consumption": delta_consumption,
            "delta_gas": delta_gas
        }

        energy_records.append(energy_record)
        previous_row = row
        next_expected_timestamp = timestamp + timedelta(minutes=5)

    print(f'{count_holes} missing timestamps')
    print(f"inserting records into {args.target}...")
    collection.drop()
    result = collection.insert_many(energy_records)
    print(f'{len(result.inserted_ids)} records inserted.')

    # close sqlite database
    conn.close()


def query_mongo_day(start,end,args):

    # reconstruction this data:
    # http://192.168.178.64:81/my_energy/api/getseries?from=2024-06-21&to=2024-06-22&resolution=Hour
    print(f'query_mongo_day({start},{end})')
    timestamp_start = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    timestamp_end = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')

    collection = get_mongodb_collection(args)

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
                'hour': {
                    '$hour': '$timestamp'
                },
                'delta_netlow': 1,
                'delta_nethigh': 1,
                'delta_gas': 1,
                'delta_consumption': 1,
                'delta_generation': 1,
                'growatt_power': 1,
                'growatt_power_today': 1,
            }
        }, {
            '$group': {
                '_id': {
                    'hour': '$hour'
                },
                'netlow': {
                    '$sum': '$delta_netlow'
                },
                'nethigh': {
                    '$sum': '$delta_nethigh'
                },
                'gas': {
                    '$sum': '$delta_gas'
                },
                'consumption': {
                    '$sum': '$delta_consumption'
                },
                'generation': {
                    '$sum': '$delta_generation'
                },
                'growatt_power': {
                    # divide by 12?
                    '$sum': '$growatt_power'
                }

            }
        }, {
            '$sort': {
                '_id.hour': 1
            }
        },
        {
            '$group': {
                '_id': None,
                'total_netlow': { '$sum': '$netlow' },
                'total_nethigh': { '$sum': '$nethigh' },
                'total_gas': { '$sum': '$gas' },
                'total_consumption': { '$sum': '$consumption' },
                'total_generation': { '$sum': '$generation' },
                'total_growatt_power': { '$sum': '$growatt_power' },
                'hourlyData': {
                    '$push': {
                        'hour': '$_id.hour',
                        'netlow': '$netlow',
                        'nethigh': '$nethigh',
                        'gas': '$gas',
                        'consumption': '$consumption',
                        'generation': '$generation',
                        'growatt_power': '$growatt_power'
                    }
                }
            }
        },
        {
            '$project': {
                '_id': 0,
                'total_netlow': 1,
                'total_nethigh': 1,
                'total_gas': 1,
                'total_consumption': 1,
                'total_generation': 1,
                'total_growatt_power': 1,
                'hourlyData': 1
            }
        }
    ]
    # Run the aggregation query
    results = list(collection.aggregate(pipeline))

    # Print the result
    for r in results:
        print(r)
        for hour in r['hourlyData']:
            print(hour)

def query_mongo_month(start,end,args):

    # reconstruction this data:
    # http://192.168.178.64:81/my_energy/api/getseries?from=2024-06-21&to=2024-06-22&resolution=Hour
    print(f'query_mongo_month({start},{end})')
    timestamp_start = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    timestamp_end = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')

    collection = get_mongodb_collection(args)

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
                'day': {
                    '$dayOfMonth': '$timestamp'
                },
                'delta_netlow': 1,
                'delta_nethigh': 1,
                'delta_gas': 1,
                'delta_consumption': 1,
                'delta_generation': 1,
                'growatt_power': 1,
                'growatt_power_today': 1,
            }
        }, {
            '$group': {
                '_id': {
                    'day': '$day'
                },
                'netlow': {
                    '$sum': '$delta_netlow'
                },
                'nethigh': {
                    '$sum': '$delta_nethigh'
                },
                'gas': {
                    '$sum': '$delta_gas'
                },
                'consumption': {
                    '$sum': '$delta_consumption'
                },
                'generation': {
                    '$sum': '$delta_generation'
                },
                'growatt_power': {
                    '$sum': '$growatt_power'
                }

            }
        }, {
            '$sort': {
                '_id.day': 1
            }
        },
        {
            '$group': {
                '_id': None,
                'total_netlow': { '$sum': '$netlow' },
                'total_nethigh': { '$sum': '$nethigh' },
                'total_gas': { '$sum': '$gas' },
                'total_consumption': { '$sum': '$consumption' },
                'total_generation': { '$sum': '$generation' },
                'total_growatt_power': { '$sum': '$growatt_power' },
                'dailyData': {
                    '$push': {
                        'day': '$_id.day',
                        'netlow': '$netlow',
                        'nethigh': '$nethigh',
                        'gas': '$gas',
                        'consumption': '$consumption',
                        'generation': '$generation',
                        'growatt_power': '$growatt_power'
                    }
                }
            }
        },
        {
            '$project': {
                '_id': 0,
                'total_netlow': 1,
                'total_nethigh': 1,
                'total_gas': 1,
                'total_consumption': 1,
                'total_generation': 1,
                'total_growatt_power': 1,
                'dailyData': 1
            }
        }
    ]
    # Run the aggregation query
    results = list(collection.aggregate(pipeline))

    # Print the result
    for r in results:
        print(r)
        for day in r['dailyData']:
            print(day)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    def get_arguments(parser):
        """
        Gets the arguments with which this application is called and returns
        the parsed arguments.
        If a argfile is give as argument, the arguments will be overrided
        The args.argfile need to be an absolute path!
        :param parser: the argument parser.
        :return: Returns the arguments.
        """
        args = parser.parse_args()
        if args.argfile:
            args_file = args.argfile
            if os.path.exists(args_file):
                parse_args_params = ['@' + args_file]
                # First add argument file
                # Now add command-line arguments to allow override of settings from file.
                for arg in sys.argv[1:]:  # Ignore first argument, since it is the path to the python script itself
                    parse_args_params.append(arg)
                print(parse_args_params)
                args = parser.parse_args(parse_args_params)
            else:
                raise (Exception("Can not find parameter file " + args_file))
        return args


    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    parser.add_argument("--command",
                        default="update-latest",
                        help="sqlite-to-mongo, update-latest, query-mongo")

    parser.add_argument("--source",
                        default="./my_energy.sqlite3",
                        help="the source file or url to read data")
    parser.add_argument("--target",
                        default="mongodb://middle-earth:27017/",
                        help="the target to write converted data to")
    parser.add_argument("--database",
                        default="my_energy",
                        help="mongodb database")
    parser.add_argument("--collection",
                        default="energy_records",
                        help="mongodb collection")
    parser.add_argument("--limit",
                        default="0",
                        help="max records to fetch")

    parser.add_argument("--batch_size",
                        default="1000",
                        help="number of records to post")



    # All parameters in a file
    parser.add_argument('--argfile',
                        nargs='?',
                        type=str,
                        help='Ascii file with arguments (overrides all other arguments')

    args = get_arguments(parser)

    print(f"--- my_energy_connector (version 29 jun 2024 ---")
    print(args)

    if args.command == "sqlite-to-mongo":
        convert_from_sqlite_to_mongo(args)

    if args.command == "query-mongo-month":
        query_mongo_month("2024-06-01 00:00:00","2024-07-01 00:00:00",args)

    if args.command == "query-mongo-day":
        query_mongo_day("2024-06-21 00:00:00","2024-06-22 00:00:00",args)