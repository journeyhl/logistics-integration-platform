from config.settings import AVS
import logging
import requests

class AddressVerificationSystem:
    def __init__(self, pipeline):
        if type(pipeline) == str:            
            self.logger = logging.getLogger(f'{pipeline}.avs')
        else:
            self.logger = logging.getLogger(f'{pipeline.pipeline_name}.avs')
        self.pipeline = pipeline
        self.account_number = AVS['account']
        self.base_uri = 'https://rest.avatax.com'
        self.endpoint_validate = f'{self.base_uri}/api/v2/addresses/resolve'
        self.license_key = AVS['license']
        self.headers = {
            "Authorization": f"Basic base64({self.account_number}:{self.license_key})",
            "Content-Type": "application/json"
        }
        self.session = requests.Session()

        pass


    def validate(self, address_data: dict):
        payload = {
            "line1": address_data['AddressLine1'],
            "textCase": "Upper",
            "city": address_data['City'],
            "region": address_data['State'],
            "country": address_data['CountryID'],
            "postalCode": address_data['PostalCode']
        }
        response = self.session.post(url = self.endpoint_validate, headers = self.headers, json = payload)
        print(f'{response.status_code}: {response.text}')
        try:
            json_response = response.json()
        except Exception as e:
            self.logger.error(f'')
            bp = 'here'
            
        bp = 'here'
