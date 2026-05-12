from . import Pipeline
from transform.notify_fulfillment_ops import Transform
from connectors import HubSpotAPI


class HubSpotProperties(Pipeline):
    def __init__(self):
        super().__init__('hubspot-properties')
        self.transformer = Transform(self)
        self.hubapi = HubSpotAPI(self)


    def extract(self):
        contacts = self.hubapi._get_properties('contacts')
        calls = self.hubapi._get_properties('calls')
        emails = self.hubapi._get_properties('emails')
        meetings = self.hubapi._get_properties('meetings')
        tasks = self.hubapi._get_properties('tasks')
        # deals = self.hubapi._get_properties('deals')
        leads = self.hubapi._get_properties('leads')
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