# build-in
import json
import logging
import requests
import time
from datetime import datetime
from ast import literal_eval

# third library

# my library
from napp_pdi.utils import get_credentials, normalize_date

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setLevel(getattr(logging, "INFO"))
formatter = logging.Formatter(
    '[%(levelname)s](%(asctime)s)%(name)s: %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.propagate = False

# TODO-1: create all requests for execute nappHUB API's
# TODO-2: create specifics errors in ../exception/napp_hub.py
# TODO-3: refactory and fix all Errors and Bugs.
# TODO-4: add and Write all DOCs, add JSON for DOCs args and specific RETURN


class NappHUB:
    _headers = {
        'Content-Type': 'application/json'
    }

    def __init__(self, server='napphub-sandbox'):
        """[summary]
        """
        # self._url_root = "https://napphub-sandbox.nappsolutions.com"
        if '.nappsolutions.com' in server:
            self._url_root = server
        else:
            self._url_root = f"https://{server}.nappsolutions.com"
        self._session = requests.Session()
        self._headers["Authorization"] = f'Bearer {self._get_token()}'
        self.store_products = None

    ############! [METHOD - GET] ############
    def _get_token(self):
        """
        [GET in route /sigin/ and return token]

        Returns:
                [str]: [token to call authenticated APIs]
        """
        time.sleep(0.1)
        
        _file_credential = get_credentials(file='napp_hub_credentials.json')

        response = self._session.post(
           f'{self._url_root}/signin/',
           data=json.dumps(_file_credential),
           headers=self._headers
        )

        token = json.loads(response.content)['token']

        return token

    def get_products(self, ):

        products = []
        url = "/products/"

        count = 0
        LIMIT = 500
        params = {
            "offset": count,
            "limit": LIMIT
        }

        response = self._session.get(
            f"{self._url_root}{url}",
            headers=self._headers,
            params=params
        )
        total = json.loads(response.content)['total']
        if total == 0:
            logger.info("/products/ is empty.")
            return []

        products.extend(json.loads(response.content)['data'])

        pages = self.pagination(total, LIMIT)
        if pages == 0:
            return products

        for i in range(0, pages, 1):
            count += LIMIT

            params["offset"] = count

            response = self._session.get(
                f"{self._url_root}{url}",
                headers=self._headers,
                params=params
            )

            products.extend(json.loads(response.content)['data'])

        return products
    
    def get_store_products(self, store_id=None):
        
        if self.store_products:
            return self.store_products
        
        store_products_all = []
        url = "/storeProducts/"

        if store_id:
            url = f'/storeProductsByStore/{store_id}'

        count = 0
        LIMIT = 500
        params = {
            "start": count,
            "end": LIMIT
        }

        response = self._session.get(
            f"{self._url_root}{url}",
            headers=self._headers,
            params=params
        )
        total = json.loads(response.content)['total']
        if total == 0:
            logger.info("/storeProducts/ is empty.")
            self.store_products = []
            return []

        store_products_all.extend(json.loads(response.content)['data'])

        pages = self.pagination(total, LIMIT)
        if pages == 0:
            self.store_products = store_products_all
            return store_products_all

        for i in range(0, pages, 1):
            count += LIMIT

            params["start"] = count

            response = self._session.get(
                f"{self._url_root}{url}",
                headers=self._headers,
                params=params
            )

            store_products_all.extend(json.loads(response.content)['data'])

        self.store_products = store_products_all
        
        return store_products_all
    
    def get_ean_products_by_erpcode(self, store_id, erp_code):
        
        product_ean = ''
        url = f"/storeProductsByErpCodeAndStore/{erp_code}/{store_id}"

        response = self._session.get(
            f"{self._url_root}{url}",
            headers=self._headers
        )
        
        if response.content and str(response.content) != "b'null'":
            data = json.loads(response.content)
            product_ean = data[0]['eanCode']['String']
        else:
            # logger.info(f"The product code {erp_code} is not registered in store {store_id}.")
            return None

        return product_ean

    def get_marketplace_stores(self, name=None):

        params = dict()
        url = 'marketplaceStores'

        if name:
            params['name'] = name
            url = 'marketplaceStoresByName'

        response = self._session.get(
            f'{self._url_root}/{url}/',
            headers=self._headers,
            params=params
        )

        return json.loads(response.content)

    def get_store_products_marketplace(self, store_id=None, marketplace_id=None):

        store_products_marketplace_all = []

        count = 0
        LIMIT = 500
        params = dict()
        params["offset"] = count
        params["limit"] = LIMIT
        params['marketplaceId'] = marketplace_id
        params['storeId'] = store_id

        response = self._session.get(
            f'{self._url_root}/storeProductsMarketplace/',
            headers=self._headers,
            params=params
        )

        total = json.loads(response.content)['total']

        if total == 0:
            logger.info("/storeProductsMarketplace/ is empty.")
            return []

        store_products_marketplace_all.extend(json.loads(response.content)['data'])

        pages = self.pagination(total, LIMIT)

        if pages == 0:
            return store_products_marketplace_all

        for i in range(0, pages, 1):
            count += LIMIT

            params["offset"] = count

            response = self._session.get(
                f"{self._url_root}/storeProductsMarketplace/",
                headers=self._headers,
                params=params)

            store_products_marketplace_all.extend(json.loads(response.content)['data'])

        return store_products_marketplace_all

    def get_compare_inventory(self, payload):

        response = self._session.get(
            f'{self._url_root}/compareInventory/',
            data=json.dumps(payload),
            headers=self._headers
        )
        return response

    def get_product_detail(self, product_id):
        response = self._session.get(
            f'{self._url_root}/products/{product_id}',
            headers=self._headers
        )
        return json.loads(response.content)

    def get_products_brands(self):
        response = self._session.get(
            f'{self._url_root}/productsBrand/',
            headers=self._headers
        )
        return json.loads(response.content)

    def get_products_by_ean(self, ean):
        response = self._session.get(
            f'{self._url_root}/productsByEan/?ean={str(ean)}',
            headers=self._headers
        )
        return json.loads(response.content)

    def get_marketplace_campaigns(self, ):
        response = self._session.get(
            f'{self._url_root}/marketplaceCampaigns/', headers=self._headers)

        return json.loads(response.content)

    def get_marketplace_campaigns_products(self, ):
        response = self._session.get(
            f'{self._url_root}/marketplaceCampaignProducts/', headers=self._headers)
        return json.loads(response.content)

    def get_categories(self, ):
        response = self._session.get(
            f'{self._url_root}/categories/', headers=self._headers)
        return json.loads(response.content)

    def get_category_name(self, name):
        response = self._session.get(
            f'{self._url_root}/categoriesByName/?name={name}', headers=self._headers)
        return json.loads(response.content)

    def get_customers(self, ):
        response = self._session.get(
            f'{self._url_root}/customers/', headers=self._headers)
        return json.loads(response.content)
    
    # https://<URL>/orderByForeignId/?foreignId=1234&storeId=197&purchasedAt=2020-09-23
    def get_orders_by_foreign_id(self, foreign_id, store_id, purchased_date):
        url = '/orderByForeignId/'
        params = {
            "foreignId": foreign_id,
            "storeId": store_id,
            "purchasedAt": datetime.strftime(normalize_date(purchased_date), "%Y-%m-%d")
        }

        response = self._session.get(
            f'{self._url_root}{url}',
            headers=self._headers,
            params=params)
        
        response = json.loads(response.content)
        
        return response
    

    def get_orders(self, store_id=None):
        url = '/orders/'
        orders_all = []

        count = 0
        LIMIT = 500
        params = {
            "start": count,
            "end": LIMIT
        }

        if store_id:
            params.update({'storeId': store_id})

        response = self._session.get(
            f'{self._url_root}{url}',
            headers=self._headers,
            params=params)

        total = json.loads(response.content)['total']
        if total == 0:
            logger.info("/orders/ is empty.")
            return []

        if json.loads(response.content)['data']:
            orders_all.extend(json.loads(response.content)['data'])

        pages = self.pagination(total, LIMIT)
        if pages == 0:
            return orders_all

        for i in range(0, pages, 1):
            count += LIMIT

            params["start"] = count

            response = self._session.get(
                f"{self._url_root}{url}",
                headers=self._headers,
                params=params
            )

            orders_all.extend(json.loads(response.content)['data'])
        return orders_all

    def get_payments(self, order_id=None):
        url = '/payments/'
        payments_all = []

        count = 0
        LIMIT = 500
        params = {
            "start": count,
            "end": LIMIT
        }

        # if order_id:
        #     params.update({'orderId': order_id})

        response = self._session.get(
            f'{self._url_root}{url}',
            headers=self._headers,
            params=params)

        total = json.loads(response.content)['total']
        if total == 0:
            logger.info("/payments/ is empty.")
            return []

        payments_all.extend(json.loads(response.content)['data'])

        pages = self.pagination(total, LIMIT)
        if pages == 0:
            return payments_all

        for i in range(0, pages, 1):
            count += LIMIT

            params["start"] = count

            response = self._session.get(
                f"{self._url_root}{url}",
                headers=self._headers,
                params=params
            )

            payments_all.extend(json.loads(response.content)['data'])
        return payments_all

    def get_shippings(self, order_id=None):
        url = '/shippings/'
        shippings_all = []

        # count = 0
        # LIMIT = 500
        # params = {
        #     "start": count,
        #     "end": LIMIT
        # }

        # if order_id:
        #     params.update({'orderId': order_id})

        response = self._session.get(
            f'{self._url_root}{url}',
            headers=self._headers,
            # params=params
            )

        # total = json.loads(response.content)['total']
        # if total == 0:
        #     logger.info("/shippings/ is empty.")
        #     return []

        # shippings_all.extend(json.loads(response.content)['data'])

        # pages = self.pagination(total, LIMIT)
        # if pages == 0:
        #     return shippings_all

        # for i in range(0, pages, 1):
        #     count += LIMIT

        #     params["start"] = count

        #     response = self._session.get(
        #         f"{self._url_root}{url}",
        #         headers=self._headers,
        #         params=params
        #     )

        #     shippings_all.extend(json.loads(response.content)['data'])
        # return shippings_all
        return json.loads(response.content)

    ############! [METHOD - POST] ############

    def post_store_products(self, payload):
        response = self._session.post(
            f"{self._url_root}/storeProducts/",
            data=json.dumps(payload),
            headers=self._headers
        )
        return response

    def post_store_orders(self, payload):
        response = self._session.post(
            f"{self._url_root}/orders/",
            data=json.dumps(payload),
            headers=self._headers
        )
        return response

    def post_store_invoices(self, payload):
        response = self._session.post(
            f"{self._url_root}/invoices/",
            data=json.dumps(payload),
            headers=self._headers
        )
        return response

    def post_marketplace_campaigns(self, payload):
        response = self._session.post(
            f'{self._url_root}/marketplaceCampaigns/',
            data=json.dumps(payload),
            headers=self._headers
        )
        return response

    def post_products_brands(self, payload):
        response = self._session.post(
            f'{self._url_root}/productsBrand/',
            data=json.dumps(payload),
            headers=self._headers
        )
        return response

    def post_products(self, payload):
        if isinstance(payload, str):
            try:
                json.loads(payload)
            except:
                logger.error("payload json invalid")
        else:
            payload = json.dumps(payload)

        response = self._session.post(
            f'{self._url_root}/products/',
            data=payload,
            headers=self._headers
        )
        return response

    def post_categories(self, payload):
        if isinstance(payload, str):
            try:
                json.loads(payload)
            except:
                logger.error("payload json invalid")
        else:
            payload = json.dumps(payload)

        response = self._session.post(
            f'{self._url_root}/categories/',
            data=payload,
            headers=self._headers
        )
        return response

    def post__product_categories(self, payload):
        if isinstance(payload, str):
            try:
                json.loads(payload)
            except:
                logger.error("payload json invalid")
        else:
            payload = json.dumps(payload)

        response = self._session.post(
            f'{self._url_root}/productCategories/',
            data=payload,
            headers=self._headers
        )
        return response

    def post_customer(self, payload):
        response = self._session.post(
            f'{self._url_root}/customers/',
            data=json.dumps(payload),
            headers=self._headers
        )
        return response

    def post_store_products_marketplace(self, payload):
        if isinstance(payload, str):
            try:
                json.loads(payload)
            except:
                logger.error("payload json invalid")
        else:
            payload = json.dumps(payload)

        response = self._session.post(
            f'{self._url_root}/storeProductsMarketplace/',
            data=payload,
            headers=self._headers
        )
        return response

    def post_payments(self, payload):
        response = self._session.post(
            f'{self._url_root}/payments/',
            data=json.dumps(payload),
            headers=self._headers
        )
        return response

    def post_shippings(self, payload):
        response = self._session.post(
            f'{self._url_root}/shippings/',
            data=json.dumps(payload),
            headers=self._headers
        )
        return response


    ############! [METHOD - PUT] ############
    def put_store_products(self, payload):
        response = self._session.put(
            f"{self._url_root}/storeProducts/",
            data=json.dumps(payload),
            headers=self._headers
        )
        return response

    def put_store_products_marketplace(self, product_id, payload):
        response = requests.put(
            f'{self._url_root}/storeProductsMarketplace/{product_id}',
            data=json.dumps(payload),
            headers=self._headers
        )

        return response

    def put_marketplace_stores(self,  payload):
        response = requests.put(
            f'{self._url_root}/marketplaceStores/',
            data=json.dumps(payload),
            headers=self._headers
        )

        return response

    def put_marketplace_campaigns(self, payload):
        response = requests.put(
            f'{self._url_root}/marketplaceCampaigns/{payload["id"]}',
            data=json.dumps(payload),
            headers=self._headers
        )

        return response

    def put_store_orders(self, payload):
        response = requests.put(
            f'{self._url_root}/orders/{payload["id"]}',
            data=json.dumps(payload),
            headers=self._headers
        )

        return response

    def put_payments(self, payload):
        response = requests.put(
            f'{self._url_root}/payments/{payload["id"]}',
            data=json.dumps(payload),
            headers=self._headers
        )

        return response

    def put_shippings(self, payload):
        # logger.info("%"*50)
        # logger.info(f'{self._url_root}/shippings/{payload["id"]}')
        response = requests.put(
            f'{self._url_root}/shippings/{payload["id"]}',
            data=json.dumps(payload),
            headers=self._headers
        )

        return response

    ############! [METHOD - PATCH] ############
    def patch_store_products_marketplace(self, product_id, payload):
        response = requests.patch(
            f'{self._url_root}/storeProductsMarketplace/{product_id}',
            data=json.dumps(payload),
            headers=self._headers
        )

        return response

    ############! [UTILS] ############
    def normalize_json_extras(self, json_extras):

        try:
            json_extras = json_extras.replace('\"', "'")
            json_extras = literal_eval(json_extras)
        except:
            json_extras = {}

        return json_extras

    def pagination(self, total_items, LIMIT):
        pages = int(total_items / LIMIT)

        return pages
