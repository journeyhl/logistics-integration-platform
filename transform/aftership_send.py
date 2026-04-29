from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipelines.aftership_send import SendToAfterShip
import logging
import polars as pl

class Transform:
    def __init__(self, pipeline: SendToAfterShip):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        pass
    
    def transform(self, data_extract: dict[str, pl.DataFrame]):
        slugs_extract = data_extract['slugs_extract']
        shipment_extract = data_extract['shipment_extract']
        shipment_extract.join(slugs_extract, on = 'Carrier')
        for i, row in enumerate(shipment_extract.iter_rows(named = True)):
            bp = 'here'
        bp = 'here'