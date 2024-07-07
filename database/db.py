from pymongo import MongoClient
from datetime import datetime,timedelta
class EnergyDB:
    def __init__(self):
        self.collection = MongoClient("mongodb://middle-earth:27017/")["my_energy"]["energy_records"]

    def get_series(self, start, end, interval, collection):
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
                        'interval': '$interval'
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
                    '_id.interval': 1
                }
            },
            {
                '$group': {
                    '_id': None,
                    'netlow': {
                        '$push': '$netlow'
                    },
                    'nethigh': {
                        '$push': '$nethigh'
                    },
                    'gas': {
                        '$push': '$gas'
                    },
                    'consumption': {
                        '$push': '$consumption'
                    },
                    'generation': {
                        '$push': '$generation'
                    },
                    'growatt_power': {
                        '$push': '$growatt_power'
                    },
                    'total_netlow': {'$sum': '$netlow'},
                    'total_nethigh': {'$sum': '$nethigh'},
                    'total_gas': {'$sum': '$gas'},
                    'total_consumption': {'$sum': '$consumption'},
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
                            'data': '$consumption',
                            'total': '$total_consumption',
                            'type': 'Consumption',
                            'energyType': 'Consumption'
                        },
                        {
                            'data': '$generation',
                            'total': '$total_generation',
                            'type': 'Generation',
                            'energyType': 'Generation'
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
        results = list(collection.aggregate(pipeline))

        return results


def get_mongodb_collection(args):
    # connect to mongodb
    return MongoClient(args.target_mongo)[args.database][args.collection]

DB = EnergyDB()