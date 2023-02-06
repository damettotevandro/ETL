# built-in
import base64
import datetime
import json
import logging
import requests
import time
from ast import literal_eval

# third library
from azure.storage.queue import (
    QueueClient,
)

# my library
from napp_pdi.utils import get_credentials
from napp_pdi.utils.hub_requests import NappHUB

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setLevel(getattr(logging, "INFO"))
formatter = logging.Formatter(
    '[%(levelname)s](%(asctime)s)%(name)s: %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.propagate = False


class AzureQueue():
    def __init__(self, account_name, queue_name, account_key):
        logger.info('Initializing Azure Queue...')
        self.account_name = account_name
        self.queue_name = queue_name
        self.account_key = account_key
        connect_str = f'DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net'

        self.queue_client = QueueClient.from_connection_string(connect_str, queue_name)

    def add_message(self, content):
        # add messages in queue
        logger.info('Message sent!')
        self.queue_client.send_message(str(content))

    def remove_message(self, message):
        logger.info('Message removed!')
        self.queue_client.delete_message(message.id, message.pop_receipt)

    def edit_message(self, message, message_edited):
        self.queue_client.update_message(
            message.id,
            message.pop_receipt,
            visibility_timeout=0,
            content=message_edited)

    def count_messages(self):
        # count queue
        properties = self.queue_client.get_queue_properties()
        count = properties.approximate_message_count
        logger.info(f'Total messages in queue ({count})')
        return int(count)

    def encode_message(self, content):
        return base64.b64encode(
            bytes(json.dumps(content), 'utf8')).decode('utf8')

    def decode_message(self, content):
        decode = base64.b64decode(content).decode('utf8')
        return json.loads(decode)

    def get_messages(self, ):
        return self.queue_client.receive_messages()
