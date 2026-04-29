from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipelines.hubspot_snapshot import HubSpotSnapshot
import logging
import polars as pl
from datetime import datetime, timedelta, timezone

class Transform:
    def __init__(self, pipeline: HubSpotSnapshot):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        self.inside_reps = ['Angela Rivieccio', 'Cameron Wright', 'Wilson Noriega', 'Annemarie Castellano']
        self.field_reps  = ['Elias Bandak', 'Kevin Massimino', 'Snowden Portis']
        pass
    
    def transform(self, data_extract: dict):
        bp = 'here'
        deals = data_extract['deals']
        owners = data_extract['owners']
        for row in deals:
            last_activity_date = datetime.strptime(row['properties']['notes_last_updated'], '%Y-%m-%dT%H:%M:%SZ')
            create_date = datetime.strptime(row['properties']['createdate'], '%Y-%m-%dT%H:%M:%S.%fZ')
            close_date = datetime.strptime(row['properties']['closedate'], '%Y-%m-%dT%H:%M:%S.%fZ')
            stage = next((stage for stage in self.pipeline.hubapi.b2b_pipeline['stages'] if stage['id'] == row['properties']['dealstage']), {})
            hubspot_owner_id = row['properties']['hubspot_owner_id']
            rep = self.pipeline.hubapi.owners.get(hubspot_owner_id)
            db_row = {
                'id': row['id'],
                'snapshot_at': data_extract['timestamp'],
'deal_id': row['properties'][''],
                'deal_name': row['properties']['dealname'],
                'stage': row['properties']['dealstage'],
                'deal_type': row['properties']['dealtype'],
                'rep_name': 'no matching owner' if not rep else rep,
                'team': 'no matching owner' if not rep else 'inside' if rep in self.inside_reps else 'field' if rep in self.field_reps else 'unknown',
                'product': row['properties']['product'],
                'amount': int(row['properties']['amount']) if type(row['properties']['amount']) == str else row['properties']['amount'],
                'lead_source': row['properties']['lead_source'],
                'order_number': row['properties']['order_number'],
                'is_true_won': 1 if stage.get('label') == 'Closed/Won' else 0,
'is_stalled': row['properties'][''],
                'create_date': create_date,
                'close_date': close_date,
                'last_activity_date': last_activity_date,
                'days_in_pipeline': (datetime.now(timezone.utc).replace(tzinfo=None) - create_date).days,
                'days_since_activity': (datetime.now(timezone.utc).replace(tzinfo=None) - last_activity_date).days,
                'closed_lost_reason': row['properties']['closed_lost_reason'],
                'primary_competitor': row['properties']['primary_competitor'],
            }
            bp = 'here'

        return deals