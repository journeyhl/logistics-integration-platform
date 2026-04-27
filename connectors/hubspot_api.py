from __future__ import annotations
from typing import Any, Iterator
from config.settings import HUBSPOT
import requests
import logging
import time


class HubSpotAPI:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.hubspot_api')
        self.base_url = 'https://api.hubapi.com'
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {HUBSPOT["access_token"]}',
            'Content-Type': 'application/json',
        })
        self.logger.info('[AUTH]  HubSpot session initialized.')

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

    def search(
        self,
        object_type: str,
        filter_groups: list[dict],
        properties: list[str],
        limit: int = 100,
    ) -> Iterator[dict]:
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

    def list_owners(self) -> dict[str, str]:
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
        return owners

    def get_deal_pipelines(self) -> list[dict]:
        data = self._request('GET', '/crm/v3/pipelines/deals')
        return data.get('results', [])

    def get_properties(self, object_type: str) -> list[dict]:
        data = self._request('GET', f'/crm/v3/properties/{object_type}')
        return data.get('results', [])
