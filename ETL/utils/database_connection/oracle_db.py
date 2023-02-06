
import logging

import cx_Oracle

logging.basicConfig(level='INFO')
logger = logging.getLogger(__name__)


class OracleDataBase:
    def __init__(self, username, password, host, port, database):
        self.username = username
        self.password = password
        self.host = host
        self.database = database
        self.port = port

    def connect(self):
        if isinstance(self.host, list):
            for host in self.host:
                params = f'{self.username}/{self.password}@{host}:{self.port}/{self.database}'
                try:
                    self._connection(params)
                    break
                except cx_Oracle.DatabaseError as e:
                    logger.error(f'Attempt host: {host} - fail')
                    continue
        else:
            params = f'{self.username}/{self.password}@{self.host}:{self.port}/{self.database}'
            self._connection(params)

    def _connection(self, params):
        try:
            self.conn = cx_Oracle.connect(params)
            self.cursor = self.conn.cursor()
            logger.info(f"Oracle Connection: {str(self.conn)[25:-1]}\nVersion Oracle: {self.conn.version}")
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if error.code == 1017:
                logger.error('Please check your credentials.')
            else:
                logger.error(f'Database connection error: {e}')
            raise cx_Oracle.DatabaseError()

    def disconnect(self):
        logger.info('Disconnected')
        try:
            self.cursor.close()
            self.conn.close()
        except cx_Oracle.DatabaseError:
            pass
        except cx_Oracle.InterfaceError:
            pass
