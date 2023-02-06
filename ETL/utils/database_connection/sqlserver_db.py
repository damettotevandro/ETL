import logging

# import pyodbc
import pymssql

logging.basicConfig(level='INFO')
logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)


class SqlServerDataBase:
    def __init__(self, username, password, host, port, database):
        self.username = username
        self.password = password
        self.host = host

        self.port = port
        self.database = database

    def connect(self):
        if isinstance(self.host, list):
            for host in self.host:
                params = {
                    'user': self.username,
                    'password': self.password,
                    'host': self.host,
                    'port': self.port,
                    'database': self.database
                }
                try:
                    self._connection(params)
                    break
                except Exception as e:
                    logger.error(f'Attempt host: {host} - fail')
                    continue
        else:
            params = {
                'user': self.username,
                'password': self.password,
                'host': self.host,
                'port': self.port,
                'database': self.database
            }
            self._connection(params)

    def _connection(self, params):
        try:
            self.conn = pymssql.connect(**params)
            self.cursor = self.conn.cursor()
            logger.info(f"SqlServer Connection: {self.host}")
        except Exception as e:
            error, = e.args
            logger.error(f'Database connection error: {e}')

    def disconnect(self):
        logger.info('Disconnected')
        try:
            self.cursor.close()
            self.conn.close()
        except Exception:
            pass
