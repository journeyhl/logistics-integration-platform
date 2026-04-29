from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipelines import SendToAfterShip, AfterShipRetrieval, UpdateAfterShip
import logging
import requests
from config.settings import AFTERSHIP
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

class AfterShip:
    def __init__(self, pipeline: SendToAfterShip | AfterShipRetrieval | UpdateAfterShip):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.aftership_api')
        self.api_key = AFTERSHIP['api_key']
        self.headers = {
            "as-api-key": self.api_key,
            "Content-Type": "application/json"
        }
        self.session = requests.Session()
        self.tracking_endpoint = 'https://api.aftership.com/tracking/2026-01/trackings'
        self.trackings = []
        pass

    def get_data(self, endpoint: str, params: dict = {}) -> dict:
        '''`get_data`(self, endpoint: *_type_*, params: *dict = {}*)
        ---
        <hr>
        
        **GET**s data from AfterShip's tracking api
        
        ### Upstream Calls 
         #### :meth:`~retrieve_trackings`
            - Initiates data retrieval process
         #### :meth:`~paginate_tracking`
            - For any result sets requiring paging, called recursively from paginate_tracking
            
        <hr>
        
        Parameters
        ---
        :param (*str*) `endpoint`: Endpoint to pull data from
        
        <hr>
        
        Returns
        ---
        :return `jresponse` (dict): dictionary containing the json formatted response from Aftership (or empty dict if unable to parse)
        '''
        try:
            response = self.session.get(url = endpoint, headers = self.headers, params = params)
        except Exception as e:
            bp = 'here'
        try:
            jresponse = response.json()
            return jresponse
        except Exception as e:
            bp = 'here'
        return {}
        

    def retrieve_trackings(self):
        '''`retrieve_trackings`(self)
        ---
        <hr>
        
        Drives the tracking retrieval process from withing the AfterShip connector.
         1. Initiates data retrieval by calling self.:meth:`~get_data` and passing :attr:`~tracking_endpoint` as the endpoint
         2. Using the response from self.:meth:`~get_data`, tracking_response, calls self.:meth:`~paginate_tracking` to parse response, extend self.:attr:`~trackings` and continue if needed
        
        ### Downstream Calls 
         #### self.:meth:`~get_data` 
            - Description
         #### self.:meth:`~paginate_tracking` 
            - Recursive function to parse the response from AfterShip's api, then page and re-call if needed
        
        ### Upstream Calls 
         #### :class:`~pipelines.aftership_update.UpdateAfterShip`.:meth:`~pipelines.aftership_update.UpdateAfterShip.extract`
            - self.:attr:`~trackings` acts as `data_extract['aftership_extract']` from :meth:`~pipelines.aftership_update.UpdateAfterShip.extract` function
            
        <hr>
        
        Returns
        ---
        :return self.:attr:`~trackings` (list): List of tracking results from Aftership api
        '''
        tracking_response = self.get_data(self.tracking_endpoint)
        self.paginate_tracking(tracking_response)
        bp = 'here'
        return self.trackings
    
    def paginate_tracking(self, tracking_response: dict):
        '''`paginate_tracking`(self, tracking_response: *dict*)
        ---
        <hr>
        
        Recursive function to parse the response from AfterShip's api, then page and re-call if needed

        (a = good response/b = bad response, p = has_next_page)
         1 ) Parses response from self.:meth:`~get_data`
         2a) Extends self.:attr:`~trackings`
         3a) Determine if we need to continue to next page or not
         4ap) Call self.:meth:`~get_data` and set its return equal to paged_tracking_response
         5ap) Enter recursive loop until `has_next_page` is false or we get a bad response
         
         2b) Do not enter recursive loop

        ### Downstream Calls 
         #### :meth:`~get_data`
            - Hits AfterShip tracking api and returns response's json if able to aprse
         #### :meth:`~paginate_tracking`
            - Only if called recursively
        
        ### Upstream Calls 
         #### :meth:`~.retrieve_trackings`
            - Entry point of function, only called from initially, any other calls are recursive
         #### :meth:`~paginate_tracking`
            - Only if called recursively
            
        <hr>
        
        Parameters
        ---
        :param (*dict*) `tracking_response`: json response from self.:meth:`~get_data`
        '''
        if tracking_response.get('meta'):
            code = tracking_response['meta']['code']
            self.logger.info(f'{code}: response recieved from Aftership')
        else:
            code = 0
            self.logger.error(f'Could not determine response status code')
        if code == 200:
            data = tracking_response['data']
            self.logger.info(f'Parsed data')
            if data.get('trackings'):
                self.trackings.extend(data['trackings'])
                self.logger.info(f'Parsed {len(self.trackings)} rows in total')
            if data.get('pagination'):
                total = data['pagination']['total']
                next_cursor = data['pagination']['next_cursor']
                has_next_page = data['pagination']['has_next_page']
                self.logger.info(f'{total} results, {'continuing' if has_next_page else 'complete'}')
                if has_next_page:
                    paged_tracking_response = self.get_data(self.tracking_endpoint, params={"cursor": next_cursor})
                    self.paginate_tracking(paged_tracking_response)


    


    def post_data(self, endpoint: str, payload: dict):
        '''`post_data`(self, endpoint: *str*, payload: *dict*)
        ---
        <hr>
        
        put_summary_here
        
        ### Downstream Calls 
         #### :meth:`~folder.file.class.method`
            - Description
        
        ### Upstream Calls 
         #### :meth:`~folder.file.class.method`
            - Description
            
        <hr>
        
        Parameters
        ---
        :param (*str*) `endpoint`: _description_
        :param (*dict*) `payload`: _description_
        
        <hr>
        
        Returns
        ---
        :return `variablename` (_type_): _description_
        '''
        try:
            response = self.session.post(url = endpoint, headers = self.headers, json = payload)
            if response.status_code == 400:
                self.parse_bad_tracking_response(response.json(), payload)
                return {}
            else:
                self.logger.info(f'Successfully posted to Aftership')
        except Exception as e:
            self.logger.error(f'Error getting response from Aftership (post_data)')
        try:
            jresponse = response.json()
            self._parse_good_tracking_response(jresponse = jresponse)
            return jresponse
        except:
            self.logger.error(f'Error parsing response from Aftership (post_data)')
        return {}
    

    def put_data(self, endpoint: str, params: dict, payload: dict, log_prefix: str = ''):
        '''`put_data`(self, endpoint: *str*, params: *dict*, payload: *dict*)
        ---
        <hr>
        
        put_summary_here
        
        ### Downstream Calls 
         #### :meth:`~folder.file.class.method`
            - Description
        
        ### Upstream Calls 
         #### :meth:`~folder.file.class.method`
            - Description
            
        <hr>
        
        Parameters
        ---
        :param (*str*) `endpoint`: _description_
        :param (*dict*) `params`: _description_
        :param (*dict*) `payload`: _description_
        
        <hr>
        
        Returns
        ---
        :return `variablename` (_type_): _description_
        '''
        self.logger.info(f'{log_prefix}')
        try:
            response = self.session.put(url = endpoint, headers = self.headers, params = params, json = payload)
        except Exception as e:
            self.logger.error(f'Error getting response from Aftership (post_data)')

        bp = 'here'
        return response








    def _parse_good_tracking_response(self, jresponse: dict):
        response_code = jresponse['meta']['code']
        msg = jresponse['meta'].get('message')
        jresponse = jresponse['data']
        id = jresponse.get('id')
        log_row = {
            'ShipmentNbr': jresponse['order_id'],
            'OrderNbr': jresponse['order_number'],
            'Tracking': jresponse['tracking_number'],
            'ResponseCode': response_code,
            'Message': msg,
            'ID': id,
            'Timestamp': datetime.now(ZoneInfo('America/New_York'))
            
        }
        self.pipeline.centralstore.checked_upsert('_util.AfterShipLog', [log_row])
        bp = 'here'



    def parse_bad_tracking_response(self, jresponse: dict, payload: dict):
        response_code = jresponse['meta']['code']
        msg = jresponse['meta']['message']
        id = jresponse['data'].get('id')
        log_row = {
            'ShipmentNbr': payload['order_id'],
            'OrderNbr': payload['order_number'],
            'Tracking': payload['tracking_number'],
            'ResponseCode': str(response_code),
            'Message': msg,
            'ID': id,
            'Timestamp': datetime.now(ZoneInfo('America/New_York'))
        }
        self.logger.warning(f'{msg}: {payload['order_number']}-{payload['order_id']}-{payload['tracking_number']}')
        self.pipeline.centralstore.checked_upsert(table_name='_util.AfterShipLog', data=[log_row])
        bp = 'here'