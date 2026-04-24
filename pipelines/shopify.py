
from pipelines import Pipeline
from connectors import ShopifyAPI
from transform.audit_fulfillment import Transform

class ShopifyGraphQL(Pipeline):
    def __init__(self):
        super().__init__('shopify-gql')
        self.shop = ShopifyAPI(self)
        self.transformer = Transform(self)


    def extract(self):
        data_extract = self.shop.post()
        return data_extract

    def transform(self, data_extract):
        data_transformed = data_extract
        return data_transformed
    
    def load(self, data_transformed):
        data_loaded = data_transformed
        return data_loaded
    
    def log_results(self, data_loaded):
        pass
