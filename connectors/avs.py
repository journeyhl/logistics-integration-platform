from config.settings import AVS
import logging
import requests
from base64 import b64encode

class AddressVerificationSystem:
    def __init__(self, pipeline):
        if type(pipeline) == str:            
            self.logger = logging.getLogger(f'{pipeline}.avs')
        else:
            self.logger = logging.getLogger(f'{pipeline.pipeline_name}.avs')
        self.pipeline = pipeline
        self.base_uri = 'https://rest.avatax.com'
        self.endpoint_validate = f'{self.base_uri}/api/v2/addresses/resolve'
        self.credentials = f"{AVS['account']}:{AVS['license']}"
        self.encoded = b64encode(self.credentials.encode()).decode()
        self.headers = {
            "Authorization": f"Basic {self.encoded}",
            "Content-Type": "application/json"
        }
        self.session = requests.Session()

        pass


    def validate(self, address_data: dict):
        payload = {
            "line1": address_data['AddressLine1'],
            "line2": address_data['AddressLine2'],
            "textCase": "Mixed",
            "city": address_data['City'],
            "region": address_data['State'],
            "country": address_data['CountryID'],
            "postalCode": address_data['PostalCode']
        }
        response = self.session.post(url = self.endpoint_validate, headers = self.headers, json = payload)
        try:
            self.json_response = response.json()
            self.logger.info(f'Response received from AVS, parsing...')
            address_data = self._parse_response(address_data)
        except Exception as e:
            self.logger.error(f'Could not reach AVS!')
            bp = 'here'
        return address_data


    def _parse_response(self, address_data: dict) -> dict:
        validated_address = self.json_response['validatedAddresses']
        if len(validated_address) > 1:
            self.logger.warning(f'Multiple addresses were returned')
            bp = 'here'
        validated_address = validated_address[0]
        address_data['vAddressLine1'] = validated_address['line1']
        address_data['vAddressLine2'] = validated_address['line2'] if validated_address['line2'] != '' else None
        address_data['vCity'] = validated_address['city']
        address_data['vState'] = validated_address['region']
        address_data['vCountryID'] = validated_address['country']
        address_data['vPostalCode'] = validated_address['postalCode']
        return address_data
