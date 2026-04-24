import logging
from config.settings import SHOPIFY
from datetime import date, timedelta
import requests

class ShopifyAPI:
    def __init__(self, pipeline):
        self.pipeline = pipeline    
        if type(pipeline) == str:            
            self.logger = logging.getLogger(f'{pipeline}.acu_api')
        else:
            self.logger = logging.getLogger(f'{pipeline.pipeline_name}.acu_api')
        self.domain = SHOPIFY['domain']
        self.version = SHOPIFY['version']
        self.token = SHOPIFY['token']
        self.key = SHOPIFY['key']
        self.secret = SHOPIFY['secret']
        self.url = f'https://{self.domain}/admin/api/{self.version}/graphql.json'
        self.headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": self.token
        }
        self.since = date(2024, 1, 1).isoformat()         # or: (date.today() - timedelta(days=90)).isoformat()
        self.until = (date.today() - timedelta(days=1)).isoformat()  # yesterday
        self._set_shopql()
        self.session = requests.Session()
        pass


    def post(self, url: str = '', headers: dict = {}, payload: dict = {}):
        url = self.url if url == '' else url
        headers = self.headers if headers == {} else headers
        payload = {"query": self.graphql_query, "variables": {"shopifyql": self.shopifyql}} if payload == {} else payload
        try:
            response = self.session.post(url = url, headers = headers, json = payload)
            response.raise_for_status()
        except Exception as e:
            bp = 'here'
        try:
            data = response.json()
        except Exception as e:
            bp = 'here'
        return data


    def _set_shopql(self):        
        self.shopifyql = f"""
        FROM sales
        SHOW gross_sales, net_sales
        WHERE is_canceled_order = false
        GROUP BY order_name, day, order_utm_source, utm_source, ...
        TIMESERIES day
        HAVING gross_sales__last_click != 0
        SINCE {self.since} UNTIL {self.until}
        ORDER BY day ASC
        """.strip()

        self.graphql_query = """
        query SalesUtmReport($shopifyql: String!) {
        shopifyqlQuery(query: $shopifyql) {
            tableData {
            columns { name displayName dataType }
            rows
            }
            parseErrors
        }
        }
        """
