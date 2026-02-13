"""
This module contains the connection handling.
"""
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine


def enable_foreign_keys(dbapi_conn):
    """
    Enable foreign key constraints for this connection.

    @param dbapi_conn: database connection
    @type dbapi_conn: L{sqlalchemy.engine.interfaces.DBAPIConnection}
    """
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


@event.listens_for(Engine, "connect")
def enable_foreign_keys_on_connect(dbapi_connection, connection_record):
    """
    Enable foreign keys if a sqlite connection has been made.

    @param dbapi_connection: database connection
    @type dbapi_connection: L{sqlalchemy.engine.interfaces.DBAPIConnection}
    @param connection_record: database connection record info
    @type connection_record: L{sqlalchemy.pool.ConnectionPoolEntry}
    """
    # Check if the connection URL is SQLite
    if "sqlite" in str(connection_record.driver_connection):
        enable_foreign_keys(dbapi_connection)


class ConnectionConfig(object):
    """
    This class holds all information for establishing a database connection.

    @ivar url: sqlalchemy database url to connect to
    @type url: L{str}
    @ivar verbose: if nonzero, be verbose
    @type verbose: L{bool}
    """
    def __init__(self, url, verbose=False):
        """
        The default constructor.

        @param url: sqlalchemy database url to connect to
        @type url: L{str}
        @param verbose: if nonzero, be verbose
        @type verbose: L{bool}
        """
        assert isinstance(url, str)

        self.url = url
        self.verbose = verbose

    def connect(self):
        """
        Establish a database connection.

        @return: the connected sqlalchemy engine
        @rtype: L{sqlalchemy.engine.Engine}
        """
        if self.verbose:
            print("Connecting to database...")
        engine = create_engine(
            self.url,
            echo=self.verbose,
        )
        if self.verbose:
            print("Connected.")
        return engine
