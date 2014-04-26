import psycopg2
import itertools

version = "0.1"

class Connection:
    def __init__(self, host, database, user=None, password=None):
        self.host = host
        self.database = database
        args = dict()
        args["database"] = database
        if user is not None:
            args["user"] = user
        if password is not None:
            args["password"] = password
        self._db = None
        self._db_args = args
        try:
            self.reconnect()
        except Exception:
            print "connect failed"

    def __del__(self):
        self.close()

    def close(self):
        if getattr(self, "_db", None) is not None:
            self._db.close()
            self._db = None

    def reconnect(self):
        self.close()
        self._db = psycopg2.connect(**self._db_args)
        self._db.autocommit = True

    def query(self, query, *parameters, **kwparameters):
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]
            return [Row(itertools.izip(column_names, row)) for row in cursor]
        finally:
            cursor.close()

    def get(self, query, *parameters, **kwparameters):
        rows = self.query(query, *parameters, **kwparameters)

        if not rows:
            return None
        elif len(rows) > 1:
            raise Exception("Method get() only supports queries with one row result,but this query actually returns more than 1 row.")
        else:
            return rows[0]
    
    def execute(self, query, *parameters, **kwparameters):
        return self.execute_rowcount(query, *parameters, **kwparameters)

    def execute_lastrowid(self, query, *parameters, **kwparameters):
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def execute_rowcount(self, query, *parameters, **kwparameters):
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.rowcount
        finally:
            cursor.close()

    def executemany(self, query, parameters):
        return self.executemany_rowcount(query, parameters)
    
    def executemany_lastrowid(self, query, parameters):
        cursor = self._cursor()
        try:
            cursor.executemany(query, parameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def executemany_rowcount(self, query, parameters):
        cursor = self._cursor()
        try:
            cursor.executemany(query, parameters)
            return cursor.rowcount
        finally:
            cursor.close()

    update = execute_rowcount
    insert = execute_lastrowid

    updatemany = executemany_rowcount
    insertmany = executemany_lastrowid
    def _cursor(self):
        return self._db.cursor()

    def _execute(self, cursor, query, parameters, kwparameters):
        try:
            cursor.execute(query, kwparameters or parameters)
        except :
            #
            self.close()
            raise


class Row(dict):
    def __getattr__(self, name):
        try:
            return self[name]
        except Exception:
            raise
