import csv
import time
import os
import json
import psycopg2 as pg
from collections import namedtuple

class DataWriterInterface:
    """ Provides a common interface for writing to CSV/SQL 
    """

    def append(self, data):
        """ Adds a row of data.
        """
        raise NotImplementedError

    def stop(self):
        """ Closes the connection to the file
        """
        raise NotImplementedError

class CSVWriter(DataWriterInterface):
    def __init__(self, column_names, file_name=None, out_directory=None):
        """ file_name defaults to `bluebird_{current time}.csv` if not provided
        """
        is_new_file = False

        if file_name == None:
            is_new_file = True
            current_time = int(time.time())
            file_name = "bluebird_{}.csv".format(current_time)  

        if out_directory:
            file_name = os.path.join(out_directory, file_name)
           
            # Create the output directory if necessary
            if not os.path.isdir(out_directory):
                os.mkdir(out_directory)

        self.source_name = file_name
        self.column_names = column_names

        # "a" opens in append mode; creates the file if it does not exist
        self._file = open(self.source_name, "a", newline="", encoding="utf8")
        self.csv_writer = csv.writer(self._file)
        
        # A new file won't have any columns, so we need to create them ourself
        if is_new_file:
            self.csv_writer.writerow(self.column_names)


    def append(self, data: namedtuple):
        self.csv_writer.writerow(list(data))

    def stop(self):
        self._file.close()

class PSQLWriter:
    def __init__(self, db_config: str):
        """
        Connects to a PostgreSQL database described in the file passed 
        as `db_config`. Must contain the keys
         - host
         - port 
         - database
         - user
         - password
         - table
        All of these are standard save `table`, which is included since this
        script can't tell which one it should write to. The provided table must 
        have the correct column names (listed below), otherwise trying to execute
        queries will error.

        Column names:
        """
        print(f"using psycopg2 version {pg.__version__}")

        self._conn = None
        dbinfo = None

        with open(db_config) as json_file:
            dbinfo = json.load(json_file)

        self.TABLENAME = dbinfo["table"]

        try:
            self._conn = pg.connect(
                host=dbinfo["host"]
                database=dbinfo["database"],
                user=dbinfo["user"],
                password=dbinfo["password"],
                port=dbinfo["port"])
        except (Exception, pg.DatabaseError) as error:
            print(f"Couldn't connect to the database: \n{error}")
            self.stop()
            raise

        self.cursor = self._conn.cursor()

    def append(self, data: namedtuple):
        """
        Inserts a scraped tweet in to the database. 
        """
        query = f"""
            INSERT INTO {self.TABLENAME}(tweetId, date, user, url, contents, weight, pos, neu, neg) 
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
        try:
            # psycopg2 handles escaping the input data
            cursor.execute(query, list(data))
        except pg.OperationalError as error:
            print(f"Error while executing query. Code: {error.pgcode} \tError: \n{error}")
            self.stop()
            raise
            
    def stop(self):
        """
        Commits changes to the database, and closes any connections
        """

        if self._conn:
            # Make changes persistant
            self._conn.commit()

            # Kill connections
            if self.cursor:
                self.cursor.close()
            self._conn.close()
