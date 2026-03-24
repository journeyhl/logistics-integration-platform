import azure.functions as af

app = af.FunctionApp()

#Send Shipments and Returns to RMI every hour at the top of the hour. 4am-11pm
@app.timer_trigger(
    schedule = '0 4-23/1 * * *',
    arg_name = 'timer',
    run_on_startup = False    
)
def rmi_send_shipment_return_pipeline(timer: af.TimerRequest):
    from pipelines import SendShipments, SendReturns
    shipment_pipeline = SendShipments()
    shipment_pipeline.run()

    return_pipeline = SendReturns()
    return_pipeline.run()

#Retrieve data from RMI api every hour, 5 minutes past. 4am-11pm
@app.timer_trigger(
    schedule = '*/5 4-23/1 * * *',
    arg_name = 'timer',
    run_on_startup = False
)
def rmi_data_retrieval_pipeline(timer: af.TimerRequest):
    from pipelines import GetClosedShipmentsFromRMI, GetReceiptsFromRMI, GetStatusFromRMI, StageRMIStatusRetrieval
    closed_shipment_pipeline = GetClosedShipmentsFromRMI()
    closed_shipment_pipeline.run()

    receipt_pipeline = GetReceiptsFromRMI()
    receipt_pipeline.run()

    rma_status_staging_pipeline = StageRMIStatusRetrieval()
    rma_numbers = rma_status_staging_pipeline['loaded']    
    status_retrieval_pipeline = GetStatusFromRMI()
    for rma_number in rma_numbers:
        status_retrieval_pipeline._re_init(rma_number = rma_number)
        status_retrieval_pipeline.run()



#Create Receipts in Acumatica at half past every hour. 8am - 8pm
@app.timer_trigger(
    schedule = '30 8-20 * * *', 
    arg_name = 'timer',
    run_on_startup = False
)
def create_acu_receipts(timer: af.TimerRequest):
    from pipelines import CreateAcuReceipt
    create_receipt_pipeline = CreateAcuReceipt()
    create_receipt_pipeline.run()
