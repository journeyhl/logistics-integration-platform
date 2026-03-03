from airflow.sdk import dag, task, BaseHook
from datetime import datetime, timedelta
from dataclasses import asdict



@dag(
    dag_id = 'send_to_rmi',
    start_date = datetime(2026, 1, 1),
    schedule = '* */1 * * 0-5',
    catchup = False,
    max_active_runs = 1
)


def rmi_pipeline():    
    @task
    def send_to_rmi(shipment):
        from pipelines import SendShipments
        rmi_pipeline = SendShipments()
        completed_rmi_pipeline = rmi_pipeline.load(shipment)
        