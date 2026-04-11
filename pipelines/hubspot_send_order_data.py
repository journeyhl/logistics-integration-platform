from . import Pipeline
from transform.notify_fulfillment_ops import Transform


class SendHubSpotOrderData(Pipeline):
    def __init__(self):
        super().__init__('send-hubspot-order-data')
        self.transformer = Transform(self)


    def extract(self):
        data_extract = self.centralstore.query_to_dataframe(self.centralstore.queries.NotifyFulfillmentOpsTeam)
        return data_extract

    def transform(self, data_extract):
        data_transformed = self.transformer.transform(data_extract)
        return data_transformed
    
    def load(self, data_transformed):
        data_loaded = data_transformed
        return data_loaded
    
    def log_results(self, data_loaded):
        pass