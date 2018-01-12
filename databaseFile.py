import csv
import os
import sqlite3
import traceback

import sys

from variables  import *

class MySqlite:

    def __init__(self):
        self.conn = None
        self.cursor = None

    def create_connection(self, db_file):
        """ create a database connection to the SQLite database
            specified by db_file
        :param db_file: database file
        :return: Connection object or None
        """
        try:
            self.conn = sqlite3.connect(db_file)
            return self.conn
        except Exception as e:
            print("Error in line : 19, file:%s\n" % __file__, e)
        return None

    def create_table(self):
        schema_table = """
        create table %s (
        %s text primary key,
        %s text 
        );
        """ % (DatabaseTableName, databaseTableColumns[0],  databaseTableColumns[1])
        self.conn.executescript(schema_table)
        self.conn.commit()

    def drop_table(self):
        schema_drop = """
        drop table if exists %s;
        """ % DatabaseTableName
        self.conn.executescript(schema_drop)
        self.conn.commit()

    def clear_table(self):
        schema_drop = """
        delete from %s;
        """ % DatabaseTableName
        self.conn.executescript(schema_drop)
        self.conn.commit()

    def update_table(self, data : DatabaseRecord):
        query_update = """
        insert into %s (%s, %s)
        values (:unique_data, :hash_code)
        """ % (DatabaseTableName, databaseTableColumns[0], databaseTableColumns[1])
        try:
            self.conn.execute(query_update, {'unique_data': data.uid, 'hash_code':data.hashcode})
            return True
        except sqlite3.IntegrityError:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            logger.error(__file__ + " : " + exc_obj + " : When Adding : " + data)
            print("Error in line : %s, file:%s" % (exc_tb.tb_lineno, __file__))
            print(exc_obj, "When adding")
            print(data)
            self.conn.rollback()
            return False

    def commit_changes(self):
        self.conn.commit()

    def query_hash(self, hashcode):
        self.cursor = self.conn.cursor()
        query_uid = """
        select %s, %s from %s
        where %s = :unique_data
        """ % (databaseTableColumns[0], databaseTableColumns[1], DatabaseTableName, databaseTableColumns[1])
        self.cursor.execute(query_uid, {'unique_data': hashcode})
        resp = self.cursor .fetchall()
        """
        for record in resp:
            unique_id, hash_code = record
            print('%-20s %-20s' % (unique_id, hash_code))
        if resp:
            return False
        else:
            return True
        """
        return resp

    def uid_unique(self, uid):
        self.cursor = self.conn.cursor()
        query_uid = """
        select %s, %s from %s
        where %s = :unique_data
        """ % (databaseTableColumns[0], databaseTableColumns[1], DatabaseTableName, databaseTableColumns[0])
        self.cursor.execute(query_uid, {'unique_data': uid})
        resp = self.cursor .fetchall()
        """
        for record in resp:
            unique_id, hash_code = record
            print('%-20s %-20s' % (unique_id, hash_code))
        """
        if resp:
            return False
        else:
            return True

"""
def get_csv_unique_id(csv_fname):
    with open(csv_fname, "r") as uid_records:
        for uid_record in csv.reader(uid_records):
            if len(uid_record) == len(uidFileColumns):  # a valid row
                yield UidRecord(*uid_record)


if __name__ == '__main__':
    uidFilename = "./uidFile.csv"
    db_filename = 'uidDatabase.db'
    database = MySqlite()

    iter_record = iter(get_csv_unique_id(uidFilename))
    record = next(iter_record)  # Skipping the column names
    recordColumns = [x.lower() for x in record]
    if not (recordColumns == uidFileColumns):
        print("No matching columns field found")
        exit(1)

    db_is_new = not os.path.exists(db_filename)
    database.create_connection(db_filename)

    if db_is_new:
        database.create_table()
    else:
        print('Clearing OLD database')
        # database.clear_table()
        database.drop_table()
        database.create_table()

    for row in iter_record:
        if database.uid_unique(row.uid):
            database.update_table(DatabaseRecord(*[row.uid, row.uid]))

    print("Database Populated")

    iter_record = iter(get_csv_unique_id(uidFilename))
    record = next(iter_record)  # Skipping the column names

    for row in iter_record:
        if database.uid_unique(row.uid):
            print("Unique Found")
        if database.update_table(DatabaseRecord(*[row.uid, row.uid])):
            print("Update Successful")
        else:
            print("Update Failure")
"""
