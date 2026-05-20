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

class LogHistory(logging.Handler):
    def __init__(self, logs: list, pipe_start: datetime, function: str):
        super().__init__()
        self.logs = logs
        self.pipe_start = pipe_start
        self.function = function
    
    def emit(self, log_entry):
        self.logs.append(self.format(log_entry))

    def format(self, log_entry):
        log_id = len(self.logs) + 1
        time = datetime.fromtimestamp(log_entry.created, ZoneInfo('America/New_York'))
        pipeline = log_entry.name if '.' not in log_entry.name else log_entry.name.split('.')[0]
        new_log_entry = {
            'AzureFunction': self.function,
            'Pipeline': pipeline,
            'LogID': log_id,
            'PipeLogName': log_entry.name,
            'FileName': log_entry.filename,
            'Method': log_entry.funcName,
            'LineNbr': log_entry.lineno,
            'PLevel': log_entry.levelno,
            'Msg': log_entry.msg,
            'Priority': log_entry.levelname,
            'Module': log_entry.module,
            'Timestamp': datetime.now(ZoneInfo('America/New_York')),
            'PipeStartTimestamp': self.pipe_start
        }
        return new_log_entry

class Pipeline(ABC):
    def __init__(self, pipeline_name: str, function: str):
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
        self.function = function
        self.centralstore: SQLConnector[CentralStoreQueries] = SQLConnector(self, 'db_CentralStore')
        self.acudb: SQLConnector[AcumaticaDbQueries] = SQLConnector(self, 'AcumaticaDb')
        self.logger = logging.getLogger(pipeline_name)
        self.logs = []
        self.run_timestamp = datetime.now(ZoneInfo('America/New_York'))
        self.logger.addHandler(LogHistory(self.logs, self.run_timestamp, self.function))        
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


        self.logger.info('Extracting...')
        data_extract = self.extract()


        self.logger.info('Transforming...')
        data_transformed = self.transform(data_extract)


        self.logger.info('Loading...')
        data_loaded = self.load(data_transformed)

        self.logger.info('Logging...')
        self.log_results(data_loaded)
        try:
            self.centralstore.insert_df(pl.DataFrame(self.logs), '_util.Logs')
        except Exception as e:
            self.logger.warning("Couldn't insert logs to SQL but pipeline execution was successful")
        return{
            'pipeline': self.pipeline_name,
            'status': 'success',
            'extracted': data_extract,
            'transformed': data_transformed,
            'loaded': data_loaded
        }