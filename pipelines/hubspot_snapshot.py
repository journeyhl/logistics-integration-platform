from pipelines.base import Pipeline
from transform.hubspot_snapshot import Transform
from connectors import HubSpotAPI
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

class HubSpotSnapshot(Pipeline):
    def __init__(self):
        super().__init__('hubspot-snapshot')
        self.hubapi = HubSpotAPI(self)
        self.transformer = Transform(self)
        self.fiscal_year_start = datetime(datetime.now(ZoneInfo('America/New_York')).year, datetime.now(ZoneInfo('America/New_York')).month, datetime.now(ZoneInfo('America/New_York')).day)
        self.week_start = datetime.now(ZoneInfo('America/New_York')).date() - timedelta(datetime.now(ZoneInfo('America/New_York')).date().weekday())
        self.month_start = datetime.now(ZoneInfo('America/New_York')).date() - timedelta(datetime.now(ZoneInfo('America/New_York')).date().day)
        pass


    def extract(self):
        owners = self.hubapi._get_owners()
        deals = self.hubapi.search_deals()
        data_extract = {
            'owners': owners,
            'deals': deals,
            'timestamp': datetime.now(ZoneInfo('America/New_York'))
        }
        return data_extract

    def transform(self, data_extract):
        data_transformed = self.transformer.transform(data_extract)
        return data_transformed
    
    def load(self, data_transformed):
        data_loaded = data_transformed
        return data_loaded
    
    def log_results(self, data_loaded):
        pass