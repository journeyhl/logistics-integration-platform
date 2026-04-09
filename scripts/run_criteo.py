import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import Criteo
from datetime import datetime, timedelta, date



criteo_pipeline = Criteo()
criteo_pipeline._re_init(
    start_date =  criteo_pipeline.incremental_end - timedelta(days=criteo_pipeline.lookback_days - 1),
    end_date = criteo_pipeline.incremental_end,
    mode = 'incremental'
)
incremental_results = criteo_pipeline.run()
bp = 'here'

criteo_pipeline._re_init(
    start_date =  date.fromisoformat('2025-01-01'),
    end_date = criteo_pipeline.backfill_end,
    mode = 'backfill'
)
backfill_results = criteo_pipeline.run()
