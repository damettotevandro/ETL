import logging

import mysql.connector


logging.basicConfig(level='INFO')
logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)


class MySqlDataBase:
    def __init__(self, username, password, host, port, database):
        self.username = username
        self.password = password
        self.host = host
        self.database = database
        self.port = port

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
                except mysql.connector.Error as e:
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
            self.conn = mysql.connector.connect(**params)
            self.cursor = self.conn.cursor()
        except mysql.connector.Error as e:
            error = e.args
            logger.error(f'Database connection error: {e}')
            raise mysql.connector.Error()
        except mysql.connector.OperationalError as e:
            if 'authentication failed' in e:
                logger.error(f'Please check your credentials: {e}')
            raise mysql.connector.OperationalError()

    def disconnect(self):
        logger.info('Disconnected')
        try:
            self.cursor.close()
            self.conn.close()
        except mysql.connector.Error:
            pass
