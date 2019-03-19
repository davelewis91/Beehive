"""
Code to simplify saving and loading spark tables
"""
import shutil
import logging as l
from glob import glob
import pyspark.sql.functions as F
from handlers import QueenBee, _byte_to_human_readable

class WorkerBee(QueenBee):
    """Use this class to make it easier to save and load tables"""
    def __init__(self, spark, source_db, analyst_db, client_url=None):
        """Initialise instance
        
        Parameters: 
           
            - spark:       spark session instance
            - source_db:   name of data source/lake
            - analyst_db:  name of user database area
        """
        self.log = l.getLogger(__name__)
        super(WorkerBee, self).__init__(client_url=client_url)
        self._spark = spark
        self._sqlcx = self._spark._wrapped
        self._user = self._spark.sparkContext.sparkUser()
        self.source_db = source_db
        self.analyst_db = analyst_db
        if not self.__check_permission(self.analyst_db):
            raise ValueError("Not an analyst database!")
        self._ds_loc = self.hdfs_location(self.source_db)
        self._aa_loc = self.hdfs_location(self.analyst_db)
        self._user_loc = self._aa_loc+self._user+'/'
        if not self.path_exists(self._user_loc):
            self.mkdir(self._user_loc)
        
    def __repr__(self):
        return ("WorkerBee(source_db={}, analyst_db={})"
                .format(self.source_db, self.analyst_db))
        
    def __check_permission(self, db):
        """Checks that database is an analyst database and can be written to
        (ie. is not EASL/Lake)"""
        not_allowed = ['inbound', 'etl', 'easl', 'lake', 'segmentations']
        if any(x in db for x in not_allowed):
            return False
        return True
        
    def _list_tables_in_hive(self, db):
        return [str(x) for x in self._sqlcx.tableNames(db)]
    
    def _list_tables_in_user_area(self, user=None):
        if not user:
            user = self._user
        return self.ls(self._aa_loc+user)
        
    def hdfs_location(self, db):
        """Find the underlying HDFS location for the database"""
        # TODO: find a way of doing this without spark
        tables = self._list_tables_in_hive(db)
        if len(tables) == 0:
            raise ValueError('Unable to find file location for database')
        loc = self._spark.table(db+'.'+tables[0])\
            .select(F.input_file_name())\
            .limit(1)\
            .collect()[0][0]  # gives parquet file path (including server address)
        loc = loc.split('/')[3:-2]  # path to database (not table)
        loc = '/'+'/'.join(loc)+'/'
        assert self.path_exists(loc), "Unable to find file location for database"
        return loc
    
    def read_table(self, table, db=None, owner=None):
        """Read in table from spark database. If db is specified, 
        reads directly from there - else works out if table in source
        or analyst database (and within user folder of analyst db)
        """
        if not db:
            # no database specified, let's find which database it's in
            if table in self._list_tables_in_hive(self.source_db):
                return self._spark.table(self.source_db+'.'+table)
            elif table in self._list_tables_in_user_area(owner):
                # use the user location to read parquet files
                if not owner:
                    owner = self._user
                return self._spark.read.parquet(self._aa_loc+owner+'/'+table)
            elif table in self._list_tables_in_hive(self.analyst_db):
                # use the hive table in analyst db directly
                return self._spark.table(self.analyst_db+'.'+table)
            else:
                raise ValueError("Cannot find table "+table)
        return self._spark.table(db+'.'+table)
    
    def save_table(self, table, table_name, mode='overwrite', partitionBy=None):
        """Saves spark dataframe table to analyst user area
        
        Parameters:
        
            - table:       spark dataframe to save
            - table_name:  name to call table
            - mode:        write mode [overwrite, error, append, ignore]
                - default: overwrite
            - partitionBy: column to partition table with
                - default: None (dataframe default)"""
        
        if mode not in ['overwrite', 'error', 'append', 'ignore']:
            raise ValueError("Write mode must be one of "
                             "['overwrite', 'error', 'append', 'ignore']")
        if not partitionBy:
            table.write.save(self._user_loc + table_name, format='parquet', 
                             mode=mode, header=True)
        else:
            table.write.save(self._user_loc + table_name, format='parquet',
                             mode=mode, header=True, partitionBy=partitionBy)
        size = self.file_size(self._user_loc+table_name)['actual']
        self.log.info(table_name + ' successfully saved! Size on disk: '
                      + _byte_to_human_readable(size))
        return
    
    def save_table_to_hive(self, table, table_name, 
                          mode='overwrite', partitionBy=None):
        """Saves spark dataframe to a Hive table in the analyst database.
        WARNING: this can lead to name conflicts, accidental loss of data,
        and other unforeseen side-effects, if you're not careful. 
        RECOMMENDED: either use save_table(), or prefix table_name with 
        your username. 
        """
        if self._user not in table_name:
            raise ValueError("Table name must contain your username!")
        if table_name in self._list_tables(self.analyst_db):
            self.log.warning("WARNING: Table already exists, overwriting")
        if not partitionBy:
            table.write.saveAsTable(self.analyst_db + '.' + table_name, 
                                    format='parquet', mode=mode)
        else:
            table.write.saveAsTable(self.analyst_db + '.' + table_name, 
                                    format='parquet', mode=mode, 
                                    partitionBy=partitionBy)
        size = self.file_size(self._aa_loc+table_name)['actual']
        self.log.info(table_name + ' successfully saved! Size on disk: '
                      + _byte_to_human_readable(size))
        return
    
    def drop_table(self, table):
        """Drops (deletes) table from user area"""
        path = self._user_loc+table
        if self.path_exists(path):
            self.delete(path, recursive=True)
        else:
            self.log.info("Could not drop table, table does not exist")
        return
    
    def export_table(self, table, path):
        """Exports table from hive to local csv"""
        if table not in self._list_tables_in_user_area():
            raise IOError("Table does not exist, cannot export it")
        if '.csv' in path:
            path = path.replace('.csv','')
        df = self.read_table(table)
        temp = self.homedir+'_tmp_'+table
        # spark writes csvs as sequences of PART files
        # reduce number of parts to 1, mv to local, take out of folder
        try:
            df.coalesce(1).write.csv(temp, header=True)
            self.mv_to_local(temp, path, overwrite=True)
        except:
            self.delete(temp, recursive=True)
            raise
        self.delete(temp, recursive=True)
        shutil.move(glob(path+'/part*')[0], path+'.csv')
        shutil.rmtree(path)
        return
        