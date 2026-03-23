from airflow.sdk import dag, task, BaseHook
from datetime import datetime

@dag(
    dag_id = 'create_acu_receipt',
    start_date = datetime(2026, 3, 1),
    schedule = '5 12-20 * * *',
    catchup = False,
    max_active_runs = 1
)
def acu_create_receipt_pipeline():

    def create_receipts():
        from pipelines import CreateAcuReceipt
        acu_receipts_pipeline = CreateAcuReceipt()
        completed_acu_receipts_pipeline = acu_receipts_pipeline.run()
        receipts = completed_acu_receipts_pipeline['loaded']
        return receipts

    create_receipts()
