from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipelines.kustomer import SendOrderDetailsToKustomer
from config.settings import KUSTOMER
import requests
import logging
from datetime import datetime, date, timedelta
import polars as pl
import io

class Kustomer:
    def __init__(self, pipeline: SendOrderDetailsToKustomer) -> None:
        pass
        self.pipeline = pipeline
        if type(pipeline) == str:
            self.logger = logging.getLogger(f'{pipeline}.kustomer_connector')
        else:
            self.logger = logging.getLogger(f'{pipeline.pipeline_name}.kustomer_connector')