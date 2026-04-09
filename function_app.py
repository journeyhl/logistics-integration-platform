import azure.functions as af
app = af.FunctionApp()

#region RMI - Send Shipments & Returns
@app.timer_trigger(
    schedule = '10/30 4-23/1 * * *',
    arg_name = 'timer',
    run_on_startup = False    
)
def rmi_send_shipment_return_pipeline(timer: af.TimerRequest):
    '''`rmi_send_shipment_return_pipeline`
    ---
    <hr>
    
    SendRMIShipments
    ===

    Queries *AcumaticaDb* for any **Open Shipments** for RMI that have **NOT** been sent to the warehouse

    Sends Shipment payload to RMI and upserts *_util.rmi_send_log*

    <hr>

    SendRMIReturns
    ===

    Queries *AcumaticaDb* for any **Open RC Orders** from RMI that have **NOT** been sent to the warehouse

    Sends Return Order payload to RMI and upserts *_util.rmi_send_log*

    <hr>

    Schedule
    ===
     *Runs at :10 and :40 every hour from 4am-11pm*
    '''
    from pipelines import SendRMIShipments, SendRMIReturns
    shipment_pipeline = SendRMIShipments()
    shipment_pipeline.run()

    return_pipeline = SendRMIReturns()
    return_pipeline.run()
#endregion RMI - Send Shipments & Returns


############################____________############################
##--------------------------     JJ     ----------------------------
############################‾‾‾‾‾‾‾‾‾‾‾‾############################


#region RMI - Data Retrieval
@app.timer_trigger(
    schedule = '25 4-23/1 * * *',
    arg_name = 'timer',
    run_on_startup = False
)
def rmi_data_retrieval_pipeline(timer: af.TimerRequest):
    '''`rmi_data_retrieval_pipeline`
    ---
    <hr>

    
    GetClosedShipmentsFromRMI
    ===

    Hits RMI's *ClosedShipmentsV1* endpoint

    Upserts to **rmi_ClosedShipments**
    
    <hr>

    GetReceiptsFromRMI
    ===

    Hits RMI's *Receipts* endpoint

    Upserts results to **rmi_Receipts**
    
    <hr>


    GetStatusFromRMI
    ===
    From CentralStore: 
    * Gets all recently pulled Closed Shipments and Receipts, 
    * Gets all recently sent Shipments & Returns

    For each row, hits RMI's rma endpoint to determine the status on their end.

    Upserts results to **RMA_Statuses**
    Gets **ClosedShipment**, **Receipt** and **Status** information from RedStag and posts to CentralStore

    <hr>

    Schedule
    ===

     *Runs at :25 every hour from 4am-11pm*
    '''
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
#endregion RMI Data Retrieval


############################____________############################
##--------------------------     JJ     ----------------------------
############################‾‾‾‾‾‾‾‾‾‾‾‾############################


#region RMI/Acu - Create Receipts
@app.timer_trigger(
    schedule = '50 8-20 * * *',
    arg_name = 'timer',
    run_on_startup = False
)
def create_acu_receipts(timer: af.TimerRequest):
    '''`create_acu_receipts`
    ---
    <hr>

    CreateAcuReceipt
    ===

    Queries CentralStore for any RMA orders with an RMAStatus of CLOSED and a DFStatus of RECEIVED
        * These orders should be Receipted in Acumatica if it's not already done so

    Queries Acudb for any RC Orders that are pending Receipt creation

    Matches Orders across datasets to find any Acumatica Orders that are ready to be Receipted.

    *For each* Matched Order:
    * Check if it has a *Receipt*(Shipment) or not via *Acumatica API*
        * If Receipt, retrieve details via *Acu API*
        * If no Receipt, create one via *Acu API* then retrieve details
    * For *each line* on the Shipment:
        * Verify the **Reason Code** is set to **RETURN**. If not, update via *Acu API*
    * If there's no **Package** *or* the # of lines on the Package != Line Details, create Package
    * Verify Shipment Details and Package Items and Quantities match
    * If all checks are passed, Confirm Shipment

    <hr>

    Schedule
    ===
     *Runs at :50 every hour from 8am-8pm*
    '''

    from pipelines import CreateAcuReceipt
    create_receipt_pipeline = CreateAcuReceipt()
    create_receipt_pipeline.run()

#endregion RMI/Acu - Create Receipts


############################____________############################
##--------------------------     JJ     ----------------------------
############################‾‾‾‾‾‾‾‾‾‾‾‾############################


#region Acu - Confirm Shipments
@app.timer_trigger(
    schedule = '*/20 4-23 * * *',
    arg_name = 'timer',
    run_on_startup = False
)
def confirm_acu_shipments(timer: af.TimerRequest):
    '''`confirm_acu_shipments`
    ---
    <hr>
    
    *1. Pulls all Open RedStag Shipments that have a Tracking Number and are ready to be confirmed*

    *2. Formats payload for Shipment Confirmation via Acumatica API*

    *3. Sends payload to confirm each Shipment*

    <hr>

    Schedule
    ===
     *Runs every 20 minutes from 4am-11pm*
    '''
    from pipelines import ShipmentsReadyToConfirm
    confirm_packed_shipments = ShipmentsReadyToConfirm()
    confirm_packed_shipments.run()
#endregion Acu - Confirm Shipments


############################____________############################
##--------------------------     JJ     ----------------------------
############################‾‾‾‾‾‾‾‾‾‾‾‾############################


#region Acu - Pack Shipments
@app.timer_trigger(
    schedule = '*/15 4-23 * * *',
    arg_name = 'timer',
    run_on_startup = False
)
def pack_shipments(timer: af.TimerRequest):
    '''`pack_shipments`
    ---
    <hr>

    *1. Queries CentralStore RedStag tables and json.RedStagEvents to find RedStag shipments ready to be packed and have tracking added.*

    >>> central_extract = self.centralstore.query_db(self.centralstore.queries.PackShipment.query)
    >>> redstag_event_extract = self.centralstore.query_db(self.centralstore.queries.RedStagEvents.query)

    *2. Queries AcumaticaDb for Open Shipments that have been sent to Warehouse but don't have a Tracking Nbr*

    >>> acu_extract = self.acudb.query_db(self.acudb.queries.PackShipment.query)
    
    *3. Matches results from Acumatica to one or both of the CentralStore extracts, then formats the Package payload to be sent to Acumatica's API*

    *4. Sends each Shipments Package Payload to Acumatica API*
    
    <hr>

    Schedule
    ===
     *Runs every 15 minutes from 4am-11pm*
    '''
    from pipelines import PackShipments
    pack_shipments = PackShipments()
    pack_shipments.run()
#endregion Acu - Pack Shipments


############################____________############################
##--------------------------     JJ     ----------------------------
############################‾‾‾‾‾‾‾‾‾‾‾‾############################


#Send Shipments and Returns to RedStag every half hour. 4am-11pm
@app.timer_trigger(
    schedule = '5/30 4-23/1 * * *',
    arg_name = 'timer',
    run_on_startup = False
)
def redstag_send_shipment_pipeline(timer: af.TimerRequest):
    '''`redstag_send_shipment_return_pipeline`
    ---
    <hr>

    SendRedStagShipments
    ===
    
    Sends Shipments to RedStag. If successful, marks as SentToWH in Acumatica along with any other attributes specified

    <hr>

    Schedule
    ===
     *Runs at :05 and :35 every hour from 4am-11pm*
    '''
    from pipelines import SendRedStagShipments
    shipment_pipeline = SendRedStagShipments()
    shipment_pipeline.run()


############################____________############################
##--------------------------     JJ     ----------------------------
############################‾‾‾‾‾‾‾‾‾‾‾‾############################

#region RedStag - Retrieve Inventory
@app.timer_trigger(
    schedule = '10 4-23/2 * * *',
    arg_name = 'timer',
    run_on_startup = False
)
def redstag_inventory_retrieval(timer: af.TimerRequest):
    '''`redstag_inventory_retrieval`
    ---
    <hr>
    
    Loads Detailed Inventory information from RedStag via API and Upserts to **RedStagInventorySummary** and **RedStagInventoryDetail**
    
    <hr>

    Schedule
    ===
     *Runs at ten after every hour from 4am-11pm*
    '''
    from pipelines import RedStagInventory
    redstag_inventory = RedStagInventory()
    redstag_inventory.run()
#endregion RedStag - Retrieve Inventory


############################____________############################
##--------------------------     JJ     ----------------------------
############################‾‾‾‾‾‾‾‾‾‾‾‾############################

#region Order Deletions
@app.timer_trigger(
    schedule = '40 * * * *',
    arg_name = 'timer',
    run_on_startup = False
)
def order_deletions(timer: af.TimerRequest):
    '''`order_deletions`
    ---
    <hr>

    Upserts Sales Orders that were deleted in Acumatica to _util.SOOrderDeletions in db_CentralStore
    
    <hr>

    Schedule
    ===
     *Runs at :40 every hour*
    '''
    from pipelines import SOOrderDeletions
    order_deletions = SOOrderDeletions()
    order_deletions.run()
#endregion Order Deletions



############################____________############################
##--------------------------     JJ     ----------------------------
############################‾‾‾‾‾‾‾‾‾‾‾‾############################

#region Address Validator
@app.timer_trigger(
    schedule = '55 * * * *',
    arg_name = 'timer',
    run_on_startup = False
)
def address_validator(timer: af.TimerRequest):
    '''`address_validator`
    ---
    <hr>

    Pulls any WB orders that are On Hold and do not have a validated address.

    Determines if the Billing and Shipping addresses are different or the same, then validates address(es) with Avalara AVS,.

    Compares Validated address(es) to original(s).

    Sends payload with updated address(es) to Acumatica API via target_api 
     - :meth:`~connectors.acu_api.AcumaticaAPI.target_api`

    Validates address(es)

    Removes hold from Order
    
    <hr>

    Schedule
    ===
     *Runs at :55 every hour*
    '''
    from pipelines import AddressValidator
    address_validator = AddressValidator()
    address_validator.run()
#endregion Address Validator