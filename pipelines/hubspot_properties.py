from . import Pipeline
from transform.notify_fulfillment_ops import Transform
from connectors import HubSpotAPI


class HubSpotProperties(Pipeline):    
    '''`HubSpotProperties`(Pipeline)
    ---
    <hr>

    Pipeline to load properties from main ObjectTypes in Hubspot to ***hs.Properties*** in db_CentralStore

    # Extraction
     - Extracts property data from Hubspot using :class:`~connectors.sql.HubSpotAPI`.:meth:`~connectors.sql.HubSpotAPI._get_properties`

    # Transformation
     - Transforms property extra into format needed for upsert to **hs.Properties**

    # Load
     - Upsert to **hs.Properties**

    # Results Logging
     - Upserts Acumatica API interactions to **_util.acu_api_log** 
    '''
    def __init__(self):
        super().__init__('hubspot-properties')
        self.transformer = Transform(self)
        self.hubapi = HubSpotAPI(self)


    def extract(self):
        contacts = self.hubapi.get_properties('contacts')
        calls = self.hubapi.get_properties('calls')
        emails = self.hubapi.get_properties('emails')
        meetings = self.hubapi.get_properties('meetings')
        tasks = self.hubapi.get_properties('tasks')
        # deals = self.hubapi._get_properties('deals')
        leads = self.hubapi.get_properties('leads')
        data_extract = contacts + calls + emails + meetings + tasks +  leads
        return data_extract

    def transform(self, data_extract):
        data_transformed = []
        for item in data_extract:
            data_transformed.append({
                'ObjectType': item['ObjectType'],
                'Name': item['name'],
                'Label': item['label'],
                'GroupName': item['groupName'],
                'Description': item['description'],
                'Type': item['type'],
                'FieldType': item['fieldType'],
                'CreatedUserId': item.get('createdUserId'),
                'UpdatedUserId': item.get('updatedUserId'),
                'DisplayOrder': item['displayOrder'],
                'Calculated': item['calculated'],
                'Archived': item.get('archived'),
                'Hidden': item['hidden'],
                'HubspotDefined': item.get('hubspotDefined'),
                'CreatedAt': item.get('createdAt'),
                'UpdatedAt': item.get('updatedAt'),
            })
        return data_transformed
    
    def load(self, data_transformed):
        self.centralstore.checked_upsert_paginated('hs.Properties', data_transformed)
        return data_transformed
    
    def log_results(self, data_loaded):
        pass