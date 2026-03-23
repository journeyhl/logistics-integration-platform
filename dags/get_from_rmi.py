from airflow.sdk import dag, task, BaseHook
from datetime import datetime, timedelta


@dag(
    dag_id = 'get_from_rmi',
    start_date = datetime(2026, 3, 1),
    schedule = '0 */1 * * *',
    catchup = False,
    max_active_runs = 1
)
def rmi_data_retrieval_pipeline():
    @task
    def get_closed_shipments():
        from pipelines import GetClosedShipmentsFromRMI
        rmi_closed_shipments_pipeline = GetClosedShipmentsFromRMI()
        completed_rmi_closed_shipments_pipeline = rmi_closed_shipments_pipeline.run()
        closed_shipments = completed_rmi_closed_shipments_pipeline['loaded']
    

    @task
    def get_receipts():
        from pipelines import GetReceiptsFromRMI
        rmi_receipts_pipeline = GetReceiptsFromRMI()
        completed_rmi_receipts_pipeline = rmi_receipts_pipeline.run()
        receipts = completed_rmi_receipts_pipeline['loaded']
    

    @task
    def stage_status_retrieval():
        from pipelines import StageRMIStatusRetrieval
        stage_status_pipeline = StageRMIStatusRetrieval()
        completed_stage_status_pipeline = stage_status_pipeline.run()
        items_to_check = completed_stage_status_pipeline['loaded']

    from pipelines import GetStatusFromRMI
    status_retrieval_pipeline = GetStatusFromRMI()
    @task
    def retrieve_status(rma_number: str):
        status_retrieval_pipeline._re_init(rma_number)
        completed_status_retrival_pipeline = status_retrieval_pipeline.run()
        status_retrieved = completed_status_retrival_pipeline['loaded']


    get_closed_shipments()
    get_receipts()

    rma_numbers_to_check = rmi_data_retrieval_pipeline()

    retrieve_status.expand(rma_number = rma_numbers_to_check)


rmi_data_retrieval_pipeline()