from abc import ABC, abstractmethod
from datetime import datetime
from zoneinfo import ZoneInfo
import logging
import polars as pl
from connectors import SQLConnector, AcumaticaAPI
from connectors.sql import CentralStoreQueries, AcumaticaDbQueries
import colorlog
from typing import TypeVar, Generic, Any

T = TypeVar('T', list, dict)

class MillisecondFormatter(colorlog.ColoredFormatter):
    def formatTime(self, record, datefmt = None):
        time = datetime.fromtimestamp(record.created)
        if datefmt:
            new_time = time.strftime(datefmt)[:-3]
            return new_time
        return time.isoformat()

class Pipeline(ABC):
    def __init__(self, pipeline_name):
        '''`init`(self, pipeline_name: *str*)
        ---
        <hr>
        
        Pipeline superclass initialization
            
        <hr>
        
        Parameters
        ---
        :param (*str*) `pipeline_name`: Name of Pipeline, passed from subclass
        
        <hr>
        
        Sets
        ---
        >>> self.pipeline_name = pipeline_name
        >>> self.centralstore =SQLConnector[CentralStoreQueries] = SQLConnector(self, 'db_CentralStore')
        >>> self.acudb = SQLConnector[AcumaticaDbQueries] = SQLConnector(self, 'AcumaticaDb')
        >>> self.logger = logging.getLogger(pipeline_name)
        '''
        self.pipeline_name = pipeline_name
        self.centralstore: SQLConnector[CentralStoreQueries] = SQLConnector(self, 'db_CentralStore')
        if pipeline_name != 'shopify-gql':
            self.acudb: SQLConnector[AcumaticaDbQueries] = SQLConnector(self, 'AcumaticaDb')
        self.logger = logging.getLogger(pipeline_name)

        
        if not logging.root.handlers:
            handler = colorlog.StreamHandler()
            handler.setFormatter(MillisecondFormatter(
                fmt='%(log_color)s%(asctime)s:  %(name)s ╍ %(levelname)s ╍ %(message)s',
                datefmt='%m/%d/%Y %H:%M:%S.%f',
                log_colors={
                    'DEBUG': 'cyan',
                    'INFO': 'white',
                    'WARNING': 'yellow',
                    'ERROR': 'red',
                    'CRITICAL': 'bold_red'
                }
            ))
            logging.root.setLevel(logging.INFO)
            logging.root.addHandler(handler)
        self.run_timestamp = None


    @abstractmethod
    def extract(self, *args, **kwargs) -> Any: ...

    @abstractmethod
    def transform(self, data_extract) -> Any: ...

    @abstractmethod
    def load(self, data_transformed) -> Any: ...

    @abstractmethod
    def log_results(self, data_loaded) -> Any: ...


    def run(self):
        self.run_timestamp = datetime.now(ZoneInfo('America/New_York'))
        self.logger.info(f'Starting {self.pipeline_name}')


        if self.pipeline_name != 'rmi-status': self.logger.info('Extracting...')
        data_extract = self.extract()


        if self.pipeline_name != 'rmi-status': self.logger.info('Transforming...')
        data_transformed = self.transform(data_extract)


        if self.pipeline_name != 'rmi-status': self.logger.info('Loading...')
        data_loaded = self.load(data_transformed)

        if self.pipeline_name != 'rmi-status': self.logger.info('Logging...')
        self.log_results(data_loaded)
        return{
            'pipeline': self.pipeline_name,
            'status': 'success',
            'extracted': data_extract,
            'transformed': data_transformed,
            'loaded': data_loaded
        }