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
        self.webhook = str(KUSTOMER['webhook'])
        self.pipeline = pipeline
        if type(pipeline) == str:
            self.logger = logging.getLogger(f'{pipeline}.kustomer_connector')
        else:
            self.logger = logging.getLogger(f'{pipeline.pipeline_name}.kustomer_connector')
        
        self.session = requests.Session()
        pass



    def target_api(self, payload_data: dict, operation: str = 'post', descr: str = None): #type: ignore
        """`target_api`(self, endpoint: *str*, payload_data: *dict*, operation: *str*, descr: *str*)
        ---
        <hr>
        
        put_summary_here
        
        <hr>
        
        Parameters
        ---
        :param (*dict*) `payload_data`: Dictionary containing **execution_payload**, **log_update_success**, **log_update_error**  
        :param (*str*) `operation`: API Operation (**PUT**, **POST**, **GET**)
        :param (*str*) `descr`: What the payload will do

        <hr>
        
        Returns
        ---
        :return `replace_me` (bool): replace_me
        """
        if operation == 'post':
            try:
                response = self.session.post(
                    url = self.webhook,
                    json=payload_data['execution_payload'],
                    headers={"Content-Type": "application/json"}
                )
                bp = 'here'
            except Exception as e:
                logging.error(f'Error! {payload_data['log_update_error']}')
                bp = 'here'
        printStr = ''

        if response.ok:
            self.logger.info(payload_data['log_update_success'])
            bp = 'here'

        else:
            logging.error(f'Error! {payload_data['log_update_error']}')
            bp = 'here'

        return response



    