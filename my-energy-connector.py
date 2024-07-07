import os,sys,argparse
import uvicorn

from api import app
from database.energy_db import DB


def run_server(args):
    """
    > my-energy-connector --command runserver
    runs a fastapi server on localhost:8000

    This can also be done like this:
    > python -m uvicorn --reload my-energy-connector:app --port 8000
    """
    uvicorn.run(app.app, host="0.0.0.0", port=8000)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    def get_arguments(parser):
        """
        Gets the arguments with which this application is called and returns
        the parsed arguments.
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

    parser.add_argument("--sqlite_database",
                        default="./my_energy.sqlite3",
                        help="the source file or url to read data")

    parser.add_argument("--start",
                        default="2024-06-21",
                        help="start date")
    parser.add_argument("--end",
                        default="2024-06-22",
                        help="end date")
    parser.add_argument("--interval",
                        default="Hour",
                        help="Hour,Day, Month, Year")

    # All parameters in a file
    parser.add_argument('--argfile',
                        nargs='?',
                        type=str,
                        help='Ascii file with arguments (overrides all other arguments')

    args = get_arguments(parser)

    print(f"--- my_energy_connector (version 7 jul 2024 ---")
    print(args)

    if args.command == "sqlite-to-mongo":
        DB.convert_from_sqlite_to_mongo(args.sqlite_database)

    if args.command == "update-to-now":
        DB.update_to_now(args.sqlite_database)

    if args.command == "getseries":
        results = DB.get_series(args.start,args.end,args.interval)

        # Print the result
        for result in results:
            for r in result['data']:
                print(r)

    if args.command == "runserver":
        run_server(args)
