import pyodata
import requests
from requests_ntlm import HttpNtlmAuth
import xmltodict
import logging

class ODATAConnector:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        self.session = requests.Session()
        self.session.auth = ('***REMOVED***', '***REMOVED***')
        pass

    def get_data(self, url: str):
        self.response = self.session.get(url).text
        self.response = xmltodict.parse(self.response)['feed']
        test = self.response
        bp = 'here'

  
if __name__ == '__main__':

    odata = ODATAConnector('pipeline')
    odata.get_data('https://erp.journeyhl.com/ODATA/JHL/JHL RMI Shipment API')
    bp = 'here'
