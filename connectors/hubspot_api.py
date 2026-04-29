from __future__ import annotations
from typing import TYPE_CHECKING,  Any, Iterator
if TYPE_CHECKING:
    from pipelines.hubspot_snapshot import HubSpotSnapshot
from config.settings import HUBSPOT
from datetime import datetime, timezone, timedelta
import requests
import logging
import time


class HubSpotAPI:
    def __init__(self, pipeline: HubSpotSnapshot):
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

    def _request(self, method: str, path: str, **kwargs) -> dict[str, Any]:
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

    def search(self, object_type: str, filter_groups: list[dict], properties: list[str], limit: int = 100) -> Iterator[dict]:
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

    def _get_owners(self) -> dict[str, str]:
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
        data = self._request('GET', f'/crm/v3/properties/{object_type}')
        return data.get('results', [])




    def search_deals(self) -> list[dict]:
        now = datetime.now(timezone.utc)
        two_years_ago_ms = str(int((now - timedelta(days=730)).timestamp() * 1000))
        fiscal_year_start_ms = str(int(self.pipeline.fiscal_year_start.timestamp() * 1000))

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
