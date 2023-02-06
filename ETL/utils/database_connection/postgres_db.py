import logging

import psycopg2

logging.basicConfig(level='INFO')
logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)


class PostgresDataBase:
    def __init__(self, username, password, host, port, database, charset=None):
        self.username = username
        self.password = password
        self.host = host
        self.database = database
        self.port = port
        self.charset = charset

    def connect(self):
        if isinstance(self.host, list):
            for host in self.host:
                params = {
                    'user': self.username,
                    'password': self.password,
                    'host': self.host,
                    'port': self.port,
                    'database': self.database,
                }
                try:
                    self._connection(params)
                    break
                except psycopg2.Error as e:
                    logger.error(f'Attempt host: {host} - fail')
                    continue
        else:
            params = {
                'user': self.username,
                'password': self.password,
                'host': self.host,
                'port': self.port,
                'database': self.database,
            }
            self._connection(params)

    def _connection(self, params):
        try:
            print(self.charset)
            self.conn = psycopg2.connect(**params)
            if self.charset:
                self.conn.set_client_encoding(self.charset)
            self.cursor = self.conn.cursor()
            resp = self.conn.get_dsn_parameters()
            logger.info(f"Postgress Connection: {resp}")
        except psycopg2.Error as e:
            error, = e.args
            logger.error(f'Database connection error: {e}')
            raise psycopg2.Error()
        except psycopg2.OperationalError as e:
            if 'authentication failed' in e:
                logger.error(f'Please check your credentials: {e}')
            raise psycopg2.OperationalError()

    def disconnect(self):
        logger.info('Disconnected')
        try:
            self.cursor.close()
            self.conn.close()
        except psycopg2.Error:
            pass
