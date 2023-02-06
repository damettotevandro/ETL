import logging

import firebirdsql
import fdb


logging.basicConfig(level='INFO')
logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)


class FirebirdDataBase:
    def __init__(self, username, password, host=None, port=None, database=None, dsn=None, charset='latin-1'):
        self.username = username
        self.password = password
        self.host = host
        self.database = database
        self.port = port
        self.charset = charset
        self.dsn = dsn

    def connect(self):
        if isinstance(self.host, list):
            for host in self.host:
                params = {
                    'user': self.username,
                    'password': self.password,
                    'host': self.host,
                    'port': self.port,
                    'database': self.database,
                    'charset': self.charset,
                    'dsn': self.dsn
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
                'database': self.database,
                'charset': self.charset,
                'dsn': self.dsn
            }
            self._connection(params)

    def _connection(self, params):
        try:
            print('Trying connect with firebirdsql')
            self.conn = firebirdsql.connect(**params)
            self.cursor = self.conn.cursor()
            logger.info(f"Firebird Connection: {self.host}")
        except Exception as e:
            try:
                print('It does not connect using firebirdsql')
                print('Trying connect with fdb')
                self.conn = fdb.connect(**params)
                self.cursor = self.conn.cursor()
                logger.info(f"Firebird Connection: {self.host}")
            except Exception as e:
                error = e.args
                logger.error(f'Database connection error: {e}')

    def disconnect(self):
        logger.info('Disconnected')
        try:
            self.cursor.close()
            self.conn.close()
        except Exception:
            pass
