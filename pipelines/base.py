from abc import ABC, abstractmethod
from datetime import datetime
import logging
import polars as pl
from connectors import SQLConnector, AcumaticaAPI
import colorlog
from typing import TypeVar, Generic

T = TypeVar('T', list, dict)

class MillisecondFormatter(colorlog.ColoredFormatter):
    def formatTime(self, time_record, datefmt = None):
        time = datetime.fromtimestamp(time_record.created)
        if datefmt:
            new_time = time.strftime(datefmt)[:-3]
            return new_time
        return time.isoformat()

class Pipeline(ABC):
    def __init__(self, pipeline_name):
        self.pipeline_name = pipeline_name
        self.centralstore = SQLConnector(self, 'db_CentralStore')
        self.acudb = SQLConnector(self, 'AcumaticaDb')
        self.acu_api = AcumaticaAPI(self)
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
    def extract(self):
        pass

    @abstractmethod
    def transform(self):
        pass

    @abstractmethod
    def load(self):
        pass


    def run(self):
        self.run_timestamp = datetime.now()
        self.logger.info(f'Starting {self.pipeline_name}')

        self.logger.info('Extracting...')
        data_extract = self.extract()




        self.logger.info('Transforming...')
        data_transformed = self.transform(data_extract)



        self.logger.info('Loading...')
        data_loaded = self.load(data_transformed)


        return{
            'pipeline': self.pipeline_name,
            'status': 'success',
            'extracted': data_extract,

        }