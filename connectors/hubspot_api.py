from __future__ import annotations
from typing import TYPE_CHECKING,  Any, Iterator
if TYPE_CHECKING:
    from pipelines import HubSpotSnapshot, HubSpotProperties
from config.settings import HUBSPOT
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import requests
import logging
import time


class HubSpotAPI:
    def __init__(self, pipeline: HubSpotSnapshot | HubSpotProperties):
        self.pipeline = pipeline
        if type(pipeline) == str:
            self.logger = logging.getLogger(f'{pipeline}.hubspot_api')
        else:
            self.logger = logging.getLogger(f'{pipeline.pipeline_name}.hubspot_api')
        self.base_url = 'https://api.hubapi.com'
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {HUBSPOT["access_token"]}',
            'Content-Type': 'application/json',
        })
        self._get_deal_pipelines()
        self._get_owners()
        self.lists = f'{self.base_url}/crm/v3/lists/'


    def _set_snapshot_windows(self):
        '''`_set_snapshot_windows`()
        ---
        <hr>
        
        Sets snapshot start windows for :class:`~pipelines.hubspot_snapshot.HubSpotSnapshot`
        
        ### Upstream Calls 
         #### :class:`~pipelines.hubspot_snapshot.HubSpotSnapshot`.:meth:`~pipelines.hubspot_snapshot.HubSpotSnapshot.__init__`
            - Called when :class:`~pipelines.hubspot_snapshot.HubSpotSnapshot` is initialized and sets snapshot windows
            
        <hr>
        
        Parameters
        ---
        
        <hr>
        
        Sets
        ---
        - #### self.:attr:`~fiscal_year_start`
        - #### self.:attr:`~week_start`
        - #### self.:attr:`~month_start`
        '''
        self.fiscal_year_start = datetime(datetime.now(ZoneInfo('America/New_York')).year, datetime.now(ZoneInfo('America/New_York')).month, datetime.now(ZoneInfo('America/New_York')).day)
        self.week_start = datetime.now(ZoneInfo('America/New_York')).date() - timedelta(datetime.now(ZoneInfo('America/New_York')).date().weekday())
        self.month_start = datetime.now(ZoneInfo('America/New_York')).date() - timedelta(days=datetime.now(ZoneInfo('America/New_York')).date().day - 1)
        

    def _request(self, method: str, path: str, **kwargs) -> dict[str, Any]:
        '''`_request`(method: *str*, path: *str*, )
        ---
        <hr>
        
        Method that actually hits the HubSpot api with the method and args passed
        
        ### Downstream Calls 
         #### :meth:`~folder.file.class.method`
            - Description
        
        ### Upstream Calls 
         #### :meth:`~_get_owners`
            - Gets distinct owners
         #### :meth:`~_get_deal_pipelines`
            - Get each different deal pipeline
         #### :meth:`~_get_properties`
            - Get all distinct properties
         #### :meth:`~search`
            - Search for the entity specified in the parameters passed
            
        <hr>
        
        Parameters
        ---
        :param (*str*) `method`: API Method to perform
        :param (*str*) `path`: API endpoint
        
        <hr>
        
        Returns
        ---
        :return `response` (dict[str, Any]): Response from HubSpot API
        '''
        url = f'{self.base_url}{path}'
        backoff = [1, 2, 4, 8, 16]
        last_status: int | None = None
        for attempt in range(5):
            response = self.session.request(method, url, timeout=30, **kwargs)
            last_status = response.status_code
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 10))
                self.logger.warning(
                    f'[RATE]  429 on {method} {path}; sleeping {retry_after}s '
                    f'(attempt {attempt + 1}/5).'
                )
                time.sleep(retry_after)
                continue
            if 500 <= response.status_code < 600:
                delay = backoff[attempt]
                self.logger.warning(
                    f'[5XX]   {response.status_code} on {method} {path}; '
                    f'sleeping {delay}s (attempt {attempt + 1}/5).'
                )
                time.sleep(delay)
                continue
            response.raise_for_status()
            return response.json()
        raise RuntimeError(
            f'HubSpot {method} {path} failed after 5 retries (last status {last_status}).'
        )
    

    def _get_owners(self) -> dict[str, str]:
        '''`_get_owners`()
        ---
        <hr>
        
        Method to retrieve Contact OwnerIDs from Hubspot API
        
        ### Downstream Calls 
         #### :meth:`~._request`
            - Method that actually hits the HubSpot API with args passed from here
        
        ### Upstream Calls 
         #### :meth:`~folder.file.class.method`
            - Description
        
        <hr>
        
        Sets
        ---
        - #### self.:attr:`~owners`
        
        <hr>
        
        Returns
        ---
        :return `owners` (dict[str, str]): list of owners returned from HubSpot
        '''
        path = '/crm/v3/owners'
        after: str | None = None
        owners: dict[str, str] = {}
        while True:
            params: dict[str, Any] = {'limit': 100}
            if after:
                params['after'] = after
            data = self._request('GET', path, params=params)
            for owner in data.get('results', []):
                name = f"{owner.get('firstName', '') or ''} {owner.get('lastName', '') or ''}".strip()
                owners[owner['id']] = name
            after = data.get('paging', {}).get('next', {}).get('after')
            if not after:
                break
        self.owners = owners
        return owners


    def _get_deal_pipelines(self) -> list[dict]:
        data = self._request('GET', '/crm/v3/pipelines/deals')
        bp = 'here'
        results = data['results']
        self.b2b_pipeline = next((result for result in results if result['label'].lower() == 'b2b'), {})
        self.b2b_closed_won = next((stage for stage in self.b2b_pipeline['stages'] if stage['label'].lower() == 'closed/won'), {})
        self.b2b_closed_lost = next((stage for stage in self.b2b_pipeline['stages'] if stage['label'].lower() == 'closed/ lost'), {})
        
        self.ecom_pipeline = next((result for result in results if result['label'].lower() == 'ecommerce pipeline'), {})
        self.inbound_pipeline = next((result for result in results if result['label'].lower() == 'inbound sales'), {})
        self.outbound_pipeline = next((result for result in results if result['label'].lower() == 'outbound sales'), {})
        return data.get('results', [])


    def get_properties(self, object_type: str) -> list[dict]:
        '''`_get_properties`(self, object_type: *str*)
        ---
        <hr>
        
        Method that drives the extraction of HubSpot properties from the **object_type** passed as a parameter
        
        ### Downstream Calls 
         #### :meth:`~_request`
            - Method that hits the HubSpot API at the endpoint we pass
        
        ### Upstream Calls 
         #### :class:`~pipelines.hubspot_properties.HubSpotProperties`.:meth:`~pipelines.hubspot_properties.HubSpotProperties.extract`
            - Description
            
        <hr>
        
        Parameters
        ---
        :param (*str*) `object_type`: Hubspot Object Type to retrieve properties for (calls, contacts, emails, meetings, etc...)
        
        <hr>
        
        Returns
        ---
        :return `results` (list[dict]): list of properties belonging to the specified object_type
        '''
        data = self._request('GET', f'/crm/v3/properties/{object_type}')
        results = data.get('results', [])
        for result in results:
            result['ObjectType'] = object_type
        # self.logger.info(f'')
        bp = 'here'
        return results
    



    def search(self, object_type: str, filter_groups: list[dict], properties: list[str], limit: int = 100) -> Iterator[dict]:
        '''`search`(self, object_type: *str*, filter_groups: *list[dict]*, properties: *list[str]*)
        ---
        <hr>
        
        Method that orchestrates how the request payload to the HubSpot API is actually delivered
        
        ### Downstream Calls 
         #### :meth:`~_request`
            - Method that goes out and hits the Hubspot API 
        
        ### Upstream Calls 
         #### :meth:`~search_deals`
            - Used to search deals
         #### :meth:`~search_activities`
            - Used to search calls, emails, meetings, tasks
         #### :meth:`~search_new_contacts`
            - Used to search for newly created contacts
            
        <hr>
        
        Parameters
        ---
        :param (*str*) `object_type`: Type of object that we are searching for (deals, calls, emails, meetings, etc)
        :param (*list[dict]*) `filter_groups`: How filtering of records should be performed
        :param (*list[str]*) `properties`: Additional properties that should be included in the response from API
        '''
        path = f'/crm/v3/objects/{object_type}/search'
        after: str | None = None
        total = 0
        while True:
            body: dict[str, Any] = {
                'filterGroups': filter_groups,
                'properties': properties,
                'limit': limit,
            }
            if after:
                body['after'] = after
            data = self._request('POST', path, json=body)
            for record in data.get('results', []):
                yield record
                total += 1
            after = data.get('paging', {}).get('next', {}).get('after')
            if not after:
                break
            if total >= 10_000:
                # HubSpot /search caps at 10k results — caller must narrow the filter.
                self.logger.error(
                    f'[CAP]   /search on {object_type} reached 10k cap; '
                    f'narrow the date range and re-query. Stopping pagination.'
                )
                break


    def search_deals(self) -> list[dict]:
        '''`search_deals`()
        ---
        <hr>
        
        put_summary_here
        
        ### Downstream Calls 
         #### :meth:`~search`
            - Orechestrates how the deal search payload will be delivered to Hubspot API

        ### Upstream Calls 
         #### :class:`~pipelines.hubspot_snapshot.HubSpotSnapshot`.:meth:`~pipelines.hubspot_snapshot.HubSpotSnapshot.extract`
            - deals -> data_extract['deals'] in HubSpotSnapshot pipeline execution
        
        <hr>
        
        Returns
        ---
        :return `deals` (list[dict]): list of deals returned from Hubspot API
        '''
        now = datetime.now(timezone.utc)
        two_years_ago_ms = str(int((now - timedelta(days=730)).timestamp() * 1000))
        fiscal_year_start_ms = str(int(self.fiscal_year_start.timestamp() * 1000))

        filter_groups = [
            {
                "filters": [
                    {"propertyName": "pipeline",   "operator": "EQ",     "value": self.b2b_pipeline['id']},
                    {"propertyName": "createdate", "operator": "GTE",    "value": two_years_ago_ms},
                    {"propertyName": "dealstage",  "operator": "NOT_IN", "values": [self.b2b_closed_won['id'], self.b2b_closed_lost['id']]},
                ]
            },
            {
                "filters": [
                    {"propertyName": "pipeline",   "operator": "EQ",  "value": self.b2b_pipeline['id']},
                    {"propertyName": "createdate", "operator": "GTE", "value": fiscal_year_start_ms},
                    {"propertyName": "dealstage",  "operator": "IN",  "values": [self.b2b_closed_won['id'], self.b2b_closed_lost['id']]},
                ]
            },
        ]

        properties = [
            "dealname", "dealstage", "dealtype", "hubspot_owner_id", "createdate",
            "product", "amount", "closedate", "hs_last_activity_date", "notes_last_updated",
            "hs_deal_is_stalled", "closed_lost_reason", "primary_competitor",
            "lead_source", "order_number", 'hs_lead_status', 'inbound_call_disposition'
        ]

        seen: set[str] = set()
        deals: list[dict] = []
        deal_result = self.search('deals', filter_groups=filter_groups, properties=properties)
        for deal in deal_result:
            if deal['id'] not in seen:
                seen.add(deal['id'])
                deals.append(deal)
        return deals


    def search_activities(self, object_type: str) -> list[dict]:
        '''`search_activities`(self, object_type: *str*)
        ---
        <hr>
        
        Method to search activities in hubspot for the ***object_type*** passed to the method
        
        ### Downstream Calls 
         #### :meth:`~search`
            - Method that actually performs the API call
            
        <hr>
        
        Parameters
        ---
        :param (*str*) `object_type`: _description_
        
        <hr>
        
        Returns
        ---
        :return `variablename` (list[dict]): Response from :meth:`~search` from the Hubspot API
        '''
        self.logger.info(f'Extracting {object_type}...')
        fiscal_year_start_ms = str(int(self.fiscal_year_start.timestamp() * 1000))
        filter_groups = [
            {"filters": [{"propertyName": "hs_timestamp", "operator": "GTE", "value": fiscal_year_start_ms}]}
        ]
        return list(self.search(object_type, filter_groups=filter_groups, properties=["hs_timestamp", "hubspot_owner_id"]))

    def search_new_contacts(self) -> list[dict]:
        '''`search_new_contacts`(self)
        ---
        <hr>
        
        Method to search Contacts in HubSpot specifically 
            
        <hr>
        
        Returns
        ---
        :return `variablename` (list[dict]): Response from :meth:`~search` containing the contacts found with the Hubspot API
        '''
        fiscal_year_start_ms = str(int(self.fiscal_year_start.timestamp() * 1000))
        filter_groups = [
            {"filters": [{"propertyName": "createdate", "operator": "GTE", "value": fiscal_year_start_ms}]}
        ]
        return list(self.search('contacts', filter_groups=filter_groups, properties=["createdate", "hubspot_owner_id"]))
    


    



    #TODO Pull in data on all the contacts
    #Track where they come from, attribute sales 
    #Here are the data points and how to structure 

    #Requirements around hubspot 