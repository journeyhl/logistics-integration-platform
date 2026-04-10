from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipelines.criteo import Criteo
import logging
import polars as pl
import pandas as pd
import re
from datetime import datetime
class Transform:
    def __init__(self, pipeline: Criteo):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        pass
    

    def landing(self, data_extract: dict[str, pl.DataFrame]):
        db_extract = data_extract['db_extract']
        criteo_extract = data_extract['criteo_extract']
        criteo_transformed = self.transform_criteo(criteo_extract)
        
        diff_log, criteo_transformed = self.find_differences(db_extract, criteo_transformed)

        data_transformed = {
            'diff_log': diff_log,
            'criteo_transformed': criteo_transformed
        }
        return data_transformed


    def transform_criteo(self, criteo_extract: pl.DataFrame):
        load_timestamp = datetime.now()
        data_transformed = criteo_extract
        # ── Drop rollup rows (Day is null or empty) ─────────────────────────
        day_col = next(
            (c for c in data_transformed.columns if c in ("Day", "date", "ï»¿day", "ï»¿date")),
            None
        )
        if day_col is None:
            raise ValueError(
                f"[TRANSFORM]  ERROR: Could not find a 'day' or 'date' column. "
            )

        before = data_transformed.height
        data_transformed = data_transformed.drop_nulls(subset=[day_col])
        data_transformed = data_transformed.filter(pl.col(day_col).cast(pl.Utf8).str.strip_chars() != "")

        self.logger.info(f"Dropped {before - data_transformed.height} rollup/aggregate row(s). "
            f"Rows remaining: {data_transformed.height}")

        if data_transformed.is_empty():
            self.logger.warning("No data rows after dropping rollups.")
            return data_transformed

        # ── Rename columns ──────────────────────────────────────────────────
        rename_map = {
            day_col:                     "report_date",
            "campaign":                  "campaign_raw",
            "Campaign":                  "campaign_raw",
            "campaignname":              "campaign_raw",    # fallback variants
            "campaign_name":             "campaign_raw",
            "clicks":                    "clicks",
            "Clicks":                    "clicks",
            "displays":                  "impressions",
            "Displays":                  "impressions",
            "advertisercost":            "cost",
            "AdvertiserCost":            "cost",
            "cost":                      "cost",            
            "Cost":                      "cost",            # fallback
            "SalesAllPc30d":             "conversions",
            "salesallpc30d":             "conversions",
            "sales":                     "conversions",     
            "Sales":                     "conversions",     # fallback
            "revenuegeneratedallpc30d":  "revenue",
            "RevenueGeneratedAllPc30d":  "revenue",
            "revenue":                   "revenue",         
            "Revenue":                   "revenue",         # fallback
            "campaignid":                "campaign_id_raw",
            "CampaignId":                "campaign_id_raw",
        }
        actual_rename = {k: v for k, v in rename_map.items() if k in data_transformed.columns}
        data_transformed = data_transformed.rename(actual_rename)

        required = ["report_date", "campaign_raw", "clicks", "impressions",
                    "cost", "conversions", "revenue"]
        missing = [c for c in required if c not in data_transformed.columns]
        if missing:
            raise ValueError(
                f"[TRANSFORM]  ERROR: Missing expected columns after rename: {missing}. "
                f"Available: {data_transformed}"
            )

        # ── Derive campaign_name — strip the ID suffix ──────────────────────
        # API returns e.g. "Power Chair Retargeting (850469)" — strip the "(ID)"
        # data_extract["campaign_name"] = data_extract["campaign_raw"].apply(
        #     lambda x: re.sub(r"\s*\(\d+\)\s*$", "", str(x)).strip()
        #     if pd.notna(x) and str(x).strip() != ""
        #     else None
        # )
        data_transformed = data_transformed.with_columns(
            pl.when(
                pl.col("campaign_raw").is_not_null()
                & (pl.col("campaign_raw").str.strip_chars() != "")
            )
            .then(
                pl.col("campaign_raw")
                .str.replace(r"\s*\(\d+\)\s*$", "")
                .str.strip_chars()
            )
            .otherwise(None)
            .alias("campaign_name")
        )

        # ── Campaign ID — use API-provided column, fall back to parsing name ──
        if 'campaign_id_raw' in data_transformed.columns:
            data_transformed = data_transformed.with_columns(
                pl.col('campaign_id_raw').cast(pl.Int64).alias('campaign_id')
            )
        else:
            data_transformed = data_transformed.with_columns(
                pl.when(
                    pl.col("campaign_raw").is_not_null()
                    & (pl.col("campaign_raw").str.strip_chars() != "")
                )
                .then(
                    pl.col('campaign_raw')
                    .str.replace(r"\s*\(\d+\)\s*$", "").str.strip_chars().cast(pl.Int64)
                )
                .otherwise(None)
                .alias('captain_id')
            )
        data_transformed.drop("campaign_raw", "campaign_id_raw")



        # ── Cast data types ─────────────────────────────────────────────────
        data_transformed = data_transformed.with_columns([
            pl.col('report_date').cast(pl.Date),
            pl.col('clicks').cast(pl.Int64, strict=False).fill_null(0),
            pl.col('impressions').cast(pl.Int64, strict=False).fill_null(0),
            pl.col('cost').cast(pl.Float64, strict=False).fill_null(0.0).round(4),
            pl.col('conversions').cast(pl.Int64, strict=False).fill_null(0),
            pl.col('revenue').cast(pl.Float64, strict=False).fill_null(0.0).round(4),
            pl.lit(self.pipeline.criteoapi.ad_id).alias('advertiser_id'),
            pl.lit(datetime.now()).alias('load_timestamp'),
        ])

        # ── Final column order ──────────────────────────────────────────────
        data_transformed = data_transformed[[
            "report_date", "advertiser_id", "campaign_id", "campaign_name",
            "impressions", "clicks", "cost", "conversions", "revenue", "load_timestamp",
        ]]

        self.logger.info(f"Final row count : {data_transformed.height}")
        self.logger.info(f"Date range      : {data_transformed['report_date'].min()}  to  {data_transformed['report_date'].max()}")
        self.logger.info(f"Campaigns found : {data_transformed['campaign_name'].n_unique()}")
        self.logger.info(f"Campaigns       : {sorted(data_transformed['campaign_name'].drop_nulls().unique().to_list())}")
        return data_transformed
    
    def find_differences(self, db_extract: pl.DataFrame, criteo_transformed: pl.DataFrame):    
        self.logger.info(f'Checking for differences in database data and Criteo API extract...')
        db_transformed = db_extract.join(other = criteo_transformed, how = 'full', on=['report_date', 'campaign_id'])
        diff_log = []
        criteo = []
        for row in db_transformed.iter_rows(named = True):
            if row['report_date'] == None:
                criteo.append(self._format_table(row))
                self.logger.info(f'{row['campaign_name_right']} - {row['report_date_right']} not found in db, set to insert')
            elif row['report_date_right'] != None:
                diff = False
                row_log = {
                    'report_date': row['report_date_right'],
                    'campaign_id': row['campaign_id'],
                    'last_ts': row['load_timestamp'],
                    'current_ts': row['load_timestamp_right']
                }
                for item in ['impressions', 'clicks', 'cost', 'conversions', 'revenue']:
                    row_log[f'{item}_diff'] = row[f'{item}_right'] - float(row[item])
                    if row_log[f'{item}_diff'] != 0:
                        diff = True
                if diff:
                    diff_log.append(row_log)
                    criteo.append(self._format_table(row))
                    self.logger.info(f'{row['campaign_name_right']} - {row['report_date_right']} found in db with different values, set to update')
        if len(criteo) == 0:
            self.logger.info(f'No changes found since last execution')
        return diff_log, criteo
    

    def _format_table(self, row: dict):
        criteo_table_format = {
            'report_date': row['report_date_right'],
            'advertiser_id': row['advertiser_id_right'],
            'campaign_id': row['campaign_id_right'],
            'campaign_name': row['campaign_name_right'],
            'impressions': row['impressions_right'],
            'clicks': row['clicks_right'],
            'cost': row['cost_right'],
            'conversions': row['conversions_right'],
            'revenue': row['revenue_right'],
            'load_timestamp': row['load_timestamp_right'],
        }
        return criteo_table_format
        