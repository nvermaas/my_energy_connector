import os,sys,argparse
import sqlite3

from pymongo import MongoClient
from datetime import datetime

def convert_from_sqlite_to_mongo(args):
    print('convert_from_sqlite_to_mongo')
    print(f'source : {args.source}')
    print(f'target : {args.target}')
    print('--------------------------')

    # connect to sqlite database (file)
    conn = sqlite3.connect(args.source)
    cur = conn.cursor()

    # connect to mongodb
    client = MongoClient(args.target)
    db = client["my_energy"]
    collection = db["energy_records"]

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
                        help="update,sqlite-to-mongo")

    parser.add_argument("--source",
                        default="./my_energy.sqlite3",
                        help="the source file or url to read data")
    parser.add_argument("--target",
                        default="mongodb://middle-earth:27017/",
                        help="the target to write converted data to")


    parser.add_argument("--limit",
                        default="0",
                        help="max records to fetch")

    parser.add_argument("--batch_size",
                        default="1000",
                        help="number of records to post")

    parser.add_argument("--collection",
                        default=None,
                        help="can be used as filter in ADEX backend and ADEX frontend")

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



