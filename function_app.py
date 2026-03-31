import azure.functions as af

app = af.FunctionApp()

#Send Shipments and Returns to RMI every half hour. 4am-11pm
@app.timer_trigger(
    schedule = '*/30 4-23/1 * * *',
    arg_name = 'timer',
    run_on_startup = False    
)
def rmi_send_shipment_return_pipeline(timer: af.TimerRequest):
    from pipelines import SendRMIShipments, SendRMIReturns
    shipment_pipeline = SendRMIShipments()
    shipment_pipeline.run()

    return_pipeline = SendRMIReturns()
    return_pipeline.run()

#Retrieve data from RMI api every hour, 5 minutes past. 4am-11pm
@app.timer_trigger(
    schedule = '5 4-23/1 * * *',
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
    rmi_statuses = rma_status_staging_pipeline.run()
    rma_numbers = rmi_statuses['loaded']    
    status_retrieval_pipeline = GetStatusFromRMI()
    for rma_number in rma_numbers:
        status_retrieval_pipeline._re_init(rma_number = rma_number)
        status_retrieval_pipeline.run()



#Create Receipts in Acumatica at 50 past every hour. 8am - 8pm
@app.timer_trigger(
    schedule = '50 8-20 * * *',
    arg_name = 'timer',
    run_on_startup = False
)
def create_acu_receipts(timer: af.TimerRequest):
    from pipelines import CreateAcuReceipt
    create_receipt_pipeline = CreateAcuReceipt()
    create_receipt_pipeline.run()



@app.timer_trigger(
    schedule = '*/20 4-23 * * *',
    arg_name = 'timer',
    run_on_startup = False
)
def confirm_acu_shipments(timer: af.TimerRequest):
    from pipelines import ShipmentsReadyToConfirm
    confirm_packed_shipments = ShipmentsReadyToConfirm()
    confirm_packed_shipments.run()


@app.timer_trigger(
    schedule = '*/15 4-23 * * *',
    arg_name = 'timer',
    run_on_startup = False
)
def pack_shipments(timer: af.TimerRequest):
    from pipelines import PackShipments
    pack_shipments = PackShipments()
    pack_shipments.run()