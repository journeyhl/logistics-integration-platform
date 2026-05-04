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
        self.month_start = datetime.now(ZoneInfo('America/New_York')).date() - timedelta(days=datetime.now(ZoneInfo('America/New_York')).date().day - 1)
        pass


    def extract(self):
        owners = self.hubapi._get_owners()
        deals = self.hubapi.search_deals()
        data_extract = {
            'owners': owners,
            'deals': deals,
            'calls':    self.hubapi.search_activities('calls'),
            'emails':   self.hubapi.search_activities('emails'),
            'meetings': self.hubapi.search_activities('meetings'),
            'tasks':    self.hubapi.search_activities('tasks'),
            'timestamp': datetime.now(ZoneInfo('America/New_York'))
        }
        return data_extract

    def transform(self, data_extract):
        data_transformed = self.transformer.transform(data_extract)
        return data_transformed
    
    def load(self, data_transformed):
        db_deals = data_transformed['db_deals']
        db_activities = data_transformed['db_activities']
        self.centralstore.checked_upsert_paginated('hs.activity_snapshots', db_activities)
        self.centralstore.checked_upsert_paginated('hs.deal_snapshots', db_deals)
        return data_transformed
    
    def log_results(self, data_loaded):
        pass