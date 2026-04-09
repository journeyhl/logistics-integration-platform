from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipelines.criteo import Criteo
from config.settings import CRITEO
import requests
import logging
from datetime import datetime, date, timedelta
import polars as pl
import io

class CriteoAPI:
    def __init__(self, pipeline: Criteo):
        self.pipeline = pipeline
        if type(pipeline) == str:
            self.logger = logging.getLogger(f'{pipeline}.criteo_api')
        else:
            self.logger = logging.getLogger(f'{pipeline.pipeline_name}.api')
        self.token_url = 'https://api.criteo.com/oauth2/token'
        self.stats_url = 'https://api.criteo.com/2026-01/statistics/report'
        self.client_id = CRITEO['client_id']
        self.client_secret = CRITEO['client_secret']
        self.ad_id = CRITEO['ad_id']
        self.headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        self.session = requests.Session()
        self.token = self._auth()

        pass

    
    def _auth(self):
        """`_auth`(self)
        ---
        <hr>

        Request a fresh Bearer token from the Criteo OAuth2 endpoint.
        Tokens are valid for 15 minutes
        
            
        <hr>
        
        Parameters
        ---
        
        <hr>
        
        Returns
        ---
        :return `token` (_type_): token for authentication
        """
        self.logger.info('[AUTH]  Requesting access token...')

        payload = {
            "grant_type":    "client_credentials",
            "client_id":     self.client_id,
            "client_secret": self.client_secret,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        try:
            response = requests.post(self.token_url, data=payload, headers=headers, timeout=30)
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            self.logger.error(f"[AUTH]  ERROR: HTTP {response.status_code} — {response.text}")
            raise
        except requests.exceptions.RequestException as e:
            self.logger.error(f"[AUTH]  ERROR: Could not reach Criteo auth endpoint — {e}")
            raise

        token = response.json().get("access_token")
        if not token:
            raise ValueError("[AUTH]  ERROR: access_token not found in response.")

        self.logger.info("[AUTH]  Token acquired successfully.")
        return token
    

        




    def fetch_campaign_data(self) -> pl.DataFrame:
        
        self.logger.info(f"[API]   Pulling {self.pipeline.start_date} to {self.pipeline.end_date}  (advertiser_id={self.ad_id})")

        payload = {
            "advertiserIds": str(self.ad_id),
            "startDate":     self.pipeline.start_date.isoformat() + "T00:00:00.000Z",
            "endDate":       self.pipeline.end_date.isoformat()   + "T00:00:00.000Z",
            "format":        "csv",
            "timezone":      "UTC",
            "currency":      "USD",
            "dimensions": [
                "Day",
                "Campaign",     # API returns campaign name + campaignid as separate columns
            ],
            "metrics": [
                "Clicks",
                "Displays",                    # impressions
                "AdvertiserCost",              # spend in USD
                "SalesAllPc30d",               # conversions (30-day post-click)
                "RevenueGeneratedAllPc30d",    # revenue (30-day post-click)
            ],
        }
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type":  "application/json",
            "Accept":        "text/csv",
        }
        try:
            response = requests.post(self.stats_url, json=payload, headers=headers, timeout=60)
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            self.logger.error(f"[API]   ERROR: HTTP {response.status_code} — {response.text}")
            raise
        except requests.exceptions.RequestException as e:
            self.logger.error(f"[API]   ERROR: Could not reach Criteo statistics endpoint — {e}")
            raise

        if not response.text.strip():
            raise ValueError("[API]   ERROR: Empty response body from statistics endpoint.")
        
        data_extract = pl.read_csv(io.StringIO(response.content.decode('utf-8-sig')), separator=";")
        self.logger.info(f"[API]   Raw rows returned: {data_extract.height}")
        return data_extract