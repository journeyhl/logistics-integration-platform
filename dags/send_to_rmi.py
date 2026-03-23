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


def rmi_send_shipment_return_pipeline():    
    @task
    def send_shipment():
        from pipelines import SendShipments
        rmi_shipment_pipeline = SendShipments()
        completed_rmi_shipment_pipeline = rmi_shipment_pipeline.run()
        rmi_shipments = completed_rmi_shipment_pipeline['loaded']
        return rmi_shipments
        
    @task
    def send_return():
        from pipelines import SendReturns
        rmi_return_pipeline = SendReturns()
        completed_rmi_return_pipeline = rmi_return_pipeline.run()
        rmi_returns = completed_rmi_return_pipeline['loaded']
        return rmi_returns
    
    shipments = send_shipment()
    returns = send_return()

rmi_send_shipment_return_pipeline()