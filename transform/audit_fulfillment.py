import logging

class Transform:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        pass
    
    def transform(self, data_extract):
        bp = 'here'
        self.pipeline