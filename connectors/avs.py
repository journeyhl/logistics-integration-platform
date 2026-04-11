from config.settings import AVS
import logging
import requests
from base64 import b64encode

class AddressVerificationSystem:
    """`AddressVerificationSystem`
    ---
    <hr>
    
    Avalara Tax Verification System API Connector 
    
    ### Methods 
     #### :meth:`~validate`
        - Given a dict (**order_data**) with Shipping and/or Billing address data, validate address with Avalara's AVS
     #### :meth:`~_parse_response`
        - Given a response from AVS, add validated address to **order_data** and return it"""    
    
    def __init__(self, pipeline):
        """`init`(self, pipeline: *Pipeline | str*)
        ---
        <hr>
        
        Initializes AdressVerificationSystem connector
        
        <hr>
        
        Parameters
        ---
        :param (*Pipeline | str*) `pipeline`: Pipeline the connector belongs to
        
        <hr>
        
        Sets
        ---
        >>> self.logger = logging.getLogger(f'{pipeline.pipeline_name}.avs')
        >>> self.pipeline = pipeline        
        >>> self.base_uri = 'https://rest.avatax.com'
        >>> self.endpoint_validate = f'{self.base_uri}/api/v2/addresses/resolve'
        >>> self.credentials = f"{AVS['account']}:{AVS['license']}"
        >>> self.encoded = b64encode(self.credentials.encode()).decode()
        >>> self.headers = {
            "Authorization": f"Basic {self.encoded}",
            "Content-Type": "application/json"
        }
        >>> self.session = requests.Session()
        """       
        self.pipeline = pipeline 
        if type(pipeline) == str:            
            self.logger = logging.getLogger(f'{pipeline}.avs')
        else:
            self.logger = logging.getLogger(f'{pipeline.pipeline_name}.avs')
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


    def validate(self, order_data: dict, s_or_b: str):
        """`validate`(self, order_data: *dict*, s_or_b: *str*)
        ---
        <hr>
        
        Given a dict with Shipping and/or Billing address data, validate address with Avalara's AVS
        
        ### Downstream Calls 
         #### :meth:`~_parse_response`
            - Given a response from AVS, add validated address to **order_data** and return it
        
        <hr>
        
        Parameters
        ---
        :param (*dict*) `order_data`: dict containing **OrderNbr** and address data
        :param (*str*) `s_or_b`: Whether we are validating the Shipping (s) or Billing (b) address
        
        <hr>
        
        Returns
        ---
        :return `order_data` (_type_): order_data dict, but with validated address included
        """        
        self.logger.info(f'{order_data['OrderNbr']}: Validating with AVS')
        payload = {
            "line1": order_data[f'{s_or_b}AddressLine1'],
            "line2": order_data[f'{s_or_b}AddressLine2'],
            "textCase": "Mixed",
            "city": order_data[f'{s_or_b}City'],
            "region": order_data[f'{s_or_b}State'],
            "country": order_data[f'{s_or_b}CountryID'],
            "postalCode": order_data[f'{s_or_b}PostalCode']
        }
        response = self.session.post(url = self.endpoint_validate, headers = self.headers, json = payload)
        try:
            self.json_response = response.json()
            self.logger.info(f'{order_data['OrderNbr']}: Response received from AVS, parsing...')
            order_data = self._parse_response(order_data, s_or_b)
        except Exception as e:
            self.logger.error(f'Could not reach AVS!')
            bp = 'here'
        return order_data


    def _parse_response(self, order_data: dict, s_or_b: str) -> dict:
        """`_parse_response`(self, order_data: *dict*, s_or_b: *str*)
        ---
        <hr>
        
        Given a response from AVS, add validated address to **order_data** and return it
        
        <hr>

        ### Upstream Calls 
         #### :meth:`~validate`
        
        <hr>
        
        Parameters
        ---
        :param (*dict*) `order_data`: dict containing **OrderNbr** and address data
        :param (*str*) `s_or_b`: Whether we are validating the Shipping (s) or Billing (b) address
        
        <hr>
        
        Returns
        ---
        :return `order_data` (dict): order_data dict, but with validated address included
        """        
        validated_address = self.json_response['validatedAddresses']
        if len(validated_address) > 1:
            self.logger.warning(f'{order_data['OrderNbr']}: Multiple addresses were returned')
            bp = 'here'
        validated_address = validated_address[0]
        order_data[f'v{s_or_b}AddressLine1'] = validated_address['line1']
        order_data[f'v{s_or_b}AddressLine2'] = validated_address['line2'] if validated_address['line2'] != None else ''
        order_data[f'v{s_or_b}City'] = validated_address['city']
        order_data[f'v{s_or_b}State'] = validated_address['region']
        order_data[f'v{s_or_b}CountryID'] = validated_address['country']
        order_data[f'v{s_or_b}PostalCode'] = validated_address['postalCode']
        return order_data
