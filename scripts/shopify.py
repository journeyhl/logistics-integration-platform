import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from connectors.shopify import ShopifyAPI
from pipelines.shopify import ShopifyGraphQL


shop = ShopifyGraphQL()

response = shop.extract()

bp = 'here'