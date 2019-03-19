"""
Code to summarise spark tables
"""
from __future__ import print_function
import subprocess
import logging as l
import pyspark.sql.functions as F

from handlers import QueenBee, _byte_to_human_readable

class CollectorBee(QueenBee):
    
    def __init__(self, spark, database, cwd=None):
        """Initialise instance"""
        self.log = l.getLogger(__name__)
        self._user = spark.sparkContext.sparkUser()
        self._spark = spark
        self._sqlcx = self._spark._wrapped
        self._hdfs = None
        self.change_database(database)
        super(CollectorBee, self).__init__(cwd)
        return
    
    def __repr__(self):
        return "CollectorBee(database={}, cwd={})".format(self.database, self._cwd)
    
    def __check_db_access(self):
        """Checks that database is an analyst database and can be written to
        (ie. is not EASL/Lake)"""
        not_allowed = ['inbound', 'etl', 'easl', 'lake', 'segmentations']
        if any(x in self.database for x in not_allowed):
            self.__write = False
        else:
            self.__write = True
        return

    def hdfs_location(self):
        """Find the underlying HDFS location for the database"""
        # TODO: find a way of doing this without spark
        tables = self.list_tables()
        if len(tables) == 0:
            raise ValueError('Unable to find file location for database')
        loc = self._spark.table(self.database+'.'+tables[0])\
            .select(F.input_file_name())\
            .limit(1)\
            .collect()[0][0]  # gives parquet file path (including server address)
        loc = loc.split('/')[3:-2]  # path to database (not table)
        loc = '/'+'/'.join(loc)+'/'
        assert self.path_exists(loc), "Unable to find file location for database"
        return loc

    def change_database(self, database):
        """Change the database to use"""
        self.database = database
        self._hdfs = self.hdfs_location()
        self.__check_db_access()
        return

    def get_table_owner(self, table):
        """Returns the owner (user ID) of table"""
        d = self._spark.sql("describe formatted "+self.database+'.'+table)
        owner = d.filter(F.col('col_name') == 'Owner').collect()[0][1]
        return owner

    def list_tables(self, contains='', db=None):
        """Returns list of all tables in database, optionally
        filtered by 'contains'"""
        if not db:
            db = self.database
        return [str(x) for x in self._sqlcx.tableNames(db) if contains in x]

    def get_user_area(self):
        """Returns location of user's area in database, and creates it
        if it doesn't already exist"""
        if not self.__write:
            raise ValueError("Not an analyst database, cannot have personal area")
        loc = self._hdfs + self._user + '/'
        try:
            self.path_exists(loc)
        except subprocess.CalledProcessError:
            # directory doesn't exist so create it
            self.mkdir(loc)
        return loc

    def my_tables(self):
        """Returns list of tables in user's directory in database"""
        loc = self.get_user_area()
        tables = self.ls(loc)
        # get rid of non-directory files
        tables = [x for x in tables if '.' not in x.split('/')[-1]]  
        return tables

    def __my_tables(self, user=None, contains=''):
        """Returns list of tables user owns in database.
        DEPRECATED: uses spark database calls, switching to HDFS"""
        if not user:
            user = self._user
        tables = []
        for table in self.list_tables(contains=contains):
            owner = self.get_table_owner(table)
            if owner == user:
                tables.append(table)
        return tables

    def table_summary(self):
        """Prints summary of tables in user directory in database"""
        sizes = {}
        for table in self.my_tables():
            # gather all stats before printing
            size = self.file_size(self.get_user_area()+'/'+table)
            size = {k:_byte_to_human_readable(v) for k,v in size.items()}
            sizes[table] = size
        self.log.info("=======")
        for table, size in sizes.items():
            # now print out all the info
            self.log.info(table + ": "
                          + size['actual']
                          + ' (' + size['replicated'] + ')')
        self.log.info("=======")
        return