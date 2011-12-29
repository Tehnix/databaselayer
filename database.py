#!/usr/bin/env python
# -*- coding: utf-8 -*-
import threading
try:
    import sqlite3
    SQLITE = True
except ImportError:
    SQLITE = False
try:
    import MySQLdb
    MYSQL = True
except ImportError:
    MYSQL = False




class Database(threading.Thread):
    """Higher level database abstraction layer.
    
    Provides a database abstraction layer, for easy use with
    multiple different database types, without the need to
    think about SQL differences.

    """
    
    def __init__(self, dbtype=None, dbname=None, dbserver=None,
                 user=None, passw=None):
        """Sets the values for the database instance"""
        try:
            dbtype
            dbname
        except:
            raise NameError('No database type or name specified!')
        self.dbtype = dbtype
        self.dbname = dbname
        
        if dbserver is not None:
            self.dbserver = dbserver
        if user is not None:
            self.user = user
        if passw is not None:
            self.passw = passw
    
    def connect(self):
        """Make the connection based on the type of database.

        Types allowed:
            SQLite
            MySQL
        
        """
        if SQLITE and self.dbtype == 'SQLite':
            self.conn = sqlite3.connect(self.dbname)
            self.cursor = self.conn.cursor()
        elif MYSQL and self.dbtype == 'MySQL':
            self.conn = MySQLdb.connect(host=self.dbserver, db=self.dbname, 
                                        user=self.user, passwd=self.passwd)
            self.cursor = self.conn.cursor()
        else:
            raise 'No database available!'

    def _keys_to_sql(self, keys={}, sep='AND '):
        """Construct the SQL filter from a dict"""
        filters = []
        self.tempValues = ()
        for field, value in keys.iteritems():
            filters.append("%s = ? " % field)
            self.tempValues = self.tempValues + (value,)
        return sep.join(filters)
    
    def _keys_to_insert_sql(self, keys={}, sep=', '):
        """Convert a dict into an SQL field value pair"""
        fields = []
        values = []
        self.tempInsertValues = () 
        for field, value in keys.iteritems():
            fields.append(field)
            values.append('?')
            self.tempInsertValues = self.tempInsertValues + (value,)
        fields = '(' + sep.join(fields) + ') '
        values = 'VALUES(' + sep.join(values) + ') '
        return fields + values
    
    def execute(self, sql=None):
        """Simply execute the given SQL"""
        if sql is not None:
            self.connect()
            try:
                self.cursor.execute(sql)
            except sqlite3.OperationalError, e:
                self.conn.rollback()
                return 'SQL Error: %s' % e
            else:
                self.lastInsertId = self.cursor.lastrowid()
                self.conn.commit()
                self.cursor.close()
        else:
            raise NameError('There was no SQL to be parsed')
    
    def fetchall(self, table=None, filters={}, add='', out='raw'):
        """Fetches all rows from database based on the filters applied. 
        
        Arg [out] specifies what the output should be:
            None   : do nothing here (simply return)
            raw    : print it
            pretty : pretty print it into ascii tables

        """
        if table is not None:
            # Construct the SQL
            sql = 'SELECT * FROM ' + table + ' WHERE ' + self._keys_to_sql(filters)
            self.connect()
            try:
                self.cursor.execute(sql + add, self.tempValues)
            except sqlite3.OperationalError, e:
                self.conn.rollback()
                del self.tempValues
                return 'SQL Error: %s' % e
            else:
                # Cleanup and return
                del self.tempValues
                result = self.cursor.fetchall()
                self.cursor.close()
                return result 
        else:
            raise NameError('Table not specified!')
    
    def fetchone(self, table=None, filters={}, out='raw'):
        """Fetches the first row from database based on the filters applied. 
        
        Arg [out] specifies what the output should be:
            None   : do nothing here (simply return)
            raw    : print it
            pretty : pretty print it into ascii tables

        """
        if table is not None:
            # Construct the SQL
            sql = 'SELECT * FROM ' + table + ' WHERE ' + self._keys_to_sql(filters)
            self.connect()
            try:
                self.cursor.execute(sql, self.tempValues)
            except sqlite3.OperationalError, e:
                del self.tempValues
                self.conn.rollback()
                return 'SQL Error: %s' % e
            else:
                # Cleanup and return
                del self.tempValues
                result = self.cursor.fetchone()
                self.cursor.close()
                return result 
        else:
            raise NameError('Table not specified!')
    
    def insert(self, table=None, data={}):
        """Inserts specified data into the database""" 
        if table is not None:
            sql = 'INSERT INTO ' + table + self._keys_to_insert_sql(data)
            self.connect()
            try:
                self.cursor.execute(sql, self.tempInsertValues)
            except sqlite3.OperationalError, e:
                self.conn.rollback()
                del self.tempInsertValues
                return 'SQL Error: %s' % e
            else:
                del self.tempInsertValues
                self.lastInsertId = self.cursor.lastrowid()
                self.conn.commit()
                self.cursor.close()
        else:
            raise NameError('Table not specified!')
    
    def update(self, table=None, data={}, filters={}):
        """Updates rows where filters apply with, given data""" 
        if table is not None:
            values = []
            data = self._keys_to_sql(data, sep=', ')
            values.append(self.tempValues)
            if filters:
                filters = ' WHERE ' + str(self._keys_to_sql(filters))
                values.append(self.tempValues)
            else:
                filters = ''
            sql = 'UPDATE ' + table + ' SET ' + data + filters
            self.connect()
            try:
                self.cursor.execute(sql, values)
            except sqlite3.OperationalError, e:
                self.conn.rollback()
                del self.tempValues
                return 'SQL Error: %s' % e
            else:
                del self.tempValues
                self.lastInsertId = self.cursor.lastrowid()
                self.conn.commit()
                self.cursor.close()
        else: 
            raise NameError('Table not specified!')
    
    def delete(self, table=None, filters={}):
        """Deletes rows where given filters apply""" 
        if table is not None:
            filters = self._keys_to_sql(filters) 
            sql = 'DELETE FROM ' + table + ' WHERE ' + filters
            self.connect()
            try:
                self.cursor.execute(sql, self.tempValues)
            except sqlite3.OperationalError, e:
                self.conn.rollback()
                del self.tempValues
                return 'SQL Error: %s' % e
            else:
                del self.tempValues
                self.lastInsertId = self.cursor.lastrowid()
                self.conn.commit()
                self.cursor.close()
        else:
            raise NameError('Table not specified!')
    
    def last_insert_id(self):
        """Gets the id from the last database insert"""
        if self.lastInsertId:
            pass
        else:
            self.lastInsertId = False
        return self.lastInsertId
    
    def count(self, table=None, filters=None):
        """Counts the rows based on the given filters"""
        if table is not None:
            # Construct the SQL
            sql = 'SELECT * FROM ' + table + ' WHERE ' + self._keys_to_sql(filters)
            print sql
            self.connect()
            try:
                self.cursor.execute(sql, self.tempValues)
            except sqlite3.OperationalError, e:
                self.conn.rollback()
                del self.tempValues
                return 'SQL Error: %s' % e
            else:
                # Cleanup and return
                del self.tempValues
                count = self.cursor.rowcount()
                self.cusor.close()
                if count < 0 or count is None:
                    count = 0
                return count
        else:
            raise NameError('Table not specified!')


# NOTE for testing purposes only
print '--- Test run ---'
db = Database('SQLite', 'file.db')
keys_ = {"field1":"val1", "field2":"val2"}
print db.count('table1', filters=keys_)
