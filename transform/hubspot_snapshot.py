from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipelines.hubspot_snapshot import HubSpotSnapshot
import logging
import polars as pl
from datetime import datetime, timedelta, timezone

def _parse_hs_date(value: str | None) -> datetime | None:
    if not value:
        return None
    for fmt in ('%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ'):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None

class Transform:
    def __init__(self, pipeline: HubSpotSnapshot):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        self.inside_reps = ['Angela Rivieccio', 'Cameron Wright', 'Wilson Noriega', 'Annemarie Castellano']
        self.field_reps  = ['Elias Bandak', 'Kevin Massimino', 'Snowden Portis']
    
    def transform(self, data_extract: dict):
        self.owner_extract = data_extract['owners']
        self.deal_extract = data_extract['deals']
        self.call_extract = data_extract['calls']
        self.email_extract = data_extract['emails']
        self.meeting_extract = data_extract['meetings']
        self.task_extract = data_extract['tasks']
        self.timestamp = data_extract['timestamp']
        self.call_counts = self.activity_counts(extract=self.call_extract, extract_name = f'{len(self.call_extract)} Calls')
        self.email_counts = self.activity_counts(extract=self.email_extract, extract_name = f'{len(self.email_extract)} Emails')
        self.meeting_counts = self.activity_counts(extract=self.meeting_extract, extract_name = f'{len(self.meeting_extract)} Meetings')
        self.task_counts = self.activity_counts(extract=self.task_extract, extract_name = f'{len(self.task_extract)} Tasks')
        db_activities = self.smash_activity_counts()
        db_deals = self.deals()
        data_transformed = {
            'db_activities': db_activities,
            'db_deals': db_deals,
        }
        return data_transformed
        
    
    def activity_counts(self, extract: list, extract_name: str):
        self.logger.info(f'Transforming {extract_name}')
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        year_start = datetime(now.year, 1, 1)
        month_start = datetime(now.year, now.month, 1)
        week_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)

        known_reps = self.inside_reps + self.field_reps
        counts = {w: {rep: 0 for rep in known_reps} for w in ('week', 'month', 'year')}

        for row in extract:
            ts = _parse_hs_date(row['properties'].get('hs_timestamp'))
            if not ts:
                continue
            owner_id = row['properties'].get('hubspot_owner_id')
            rep = self.pipeline.hubapi.owners.get(owner_id) if owner_id else None
            if not rep:
                continue
            for window, start in (('week', week_start), ('month', month_start), ('year', year_start)):
                if ts >= start:
                    counts[window][rep] = counts[window].get(rep, 0) + 1
        return counts

    def smash_activity_counts(self):
        counts = (self.call_counts, self.email_counts, self.meeting_counts, self.task_counts)
        rows = []
        all_reps = set(self.inside_reps) | set(self.field_reps)
        for c in counts:
            bp = 'here'
            for window_counts in c.values():
                all_reps.update(window_counts.keys())
                bp = 'here'
            
        for window in ('week', 'month', 'year'):
            for rep in sorted(all_reps):
                team = 'inside' if rep in self.inside_reps else 'field' if rep in self.field_reps else 'unknown'
                rows.append({
                    'snapshot_at': self.timestamp,
                    'rep_name': rep,
                    'team': team,
                    'window': window,
                    'call_count':    self.call_counts[window].get(rep, 0),
                    'email_count':   self.email_counts[window].get(rep, 0),
                    'meeting_count': self.meeting_counts[window].get(rep, 0),
                    'task_count':    self.task_counts[window].get(rep, 0),
                    'new_contact_count': 0,
                })
        return rows



    def deals(self):
        db_deals = []
        for row in self.deal_extract:
            last_activity_date = _parse_hs_date(row['properties']['notes_last_updated'])
            days_since_activity = (datetime.now(timezone.utc).replace(tzinfo=None) - last_activity_date).days if last_activity_date else None
            create_date = _parse_hs_date(row['properties']['createdate'])
            close_date = _parse_hs_date(row['properties']['closedate'])
            stage = next((stage for stage in self.pipeline.hubapi.b2b_pipeline['stages'] if stage['id'] == row['properties']['dealstage']), {})
            hubspot_owner_id = row['properties']['hubspot_owner_id']
            rep = self.pipeline.hubapi.owners.get(hubspot_owner_id)
            db_row = {
                'snapshot_at': self.timestamp,
                'deal_id': row['id'],
                'deal_name': row['properties']['dealname'],
                'stage': row['properties']['dealstage'],
                'deal_type': row['properties']['dealtype'],
                'rep_name': 'no matching owner' if not rep else rep,
                'team': 'no matching owner' if not rep else 'inside' if rep in self.inside_reps else 'field' if rep in self.field_reps else 'unknown',
                'product': row['properties']['product'],
                'amount': float(row['properties']['amount']) if row['properties']['amount'] is not None else None,
                'lead_source': row['properties']['lead_source'],
                'order_number': row['properties']['order_number'],
                'is_true_won': 1 if stage.get('label') == 'Closed/Won' else 0,
                'is_stalled': row['properties'].get('hs_deal_is_stalled'),
                'create_date': create_date,
                'close_date': close_date,
                'last_activity_date': last_activity_date,
                'days_in_pipeline': (datetime.now(timezone.utc).replace(tzinfo=None) - create_date).days if create_date else None,
                'days_since_activity': days_since_activity,
                'closed_lost_reason': row['properties']['closed_lost_reason'],
                'primary_competitor': row['properties']['primary_competitor'],
            }
            db_deals.append(db_row)

        return db_deals