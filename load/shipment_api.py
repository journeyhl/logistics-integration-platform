from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipelines.create_acu_receipt import CreateAcuReceipt
    from pipelines.pack_shipments import PackShipments
    from pipelines.redstag_send_shipments import SendRedStagShipments
import logging
import time

class Load:
    '''Load
    ===
    <hr>

    Class for smart handling of Acumatica API interactions 
    
    '''
    def __init__(self, pipeline: CreateAcuReceipt | PackShipments | SendRedStagShipments):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.load')

    def load_shipments(self, data_transformed: dict):
        '''`load_shipments`(self, data_transformed: *dict*)
        ---
        <hr>
        
        Iterates through **data_transformed** items and adds Package for each Shipment in Acumatica if it meets all conditions
        
        <hr>
        
        Parameters
        ----------
        
        :param data_transformed: dict of Shipments to pack in Acumatica. Each entry's key is the *ShipmentNbr* and will contain **PackagePayload**
        :type data_transformed: dict
        
        <hr>
        
        
        '''
        for shipment, data in data_transformed.items():
            shipment_data = self.pipeline.acu_api.shipment_details(data)
            if (shipment_data['package_count'] == 0
            or shipment_data['package_count'] != shipment_data['line_count']
            # or len(shipment_data['PackageLines']) != shipment_data['package_count']
            # or len(shipment_data['PackageLines']) != shipment_data['line_count']
            ):
                shipment_data = self.pipeline.acu_api.add_package_v2(shipment_data)
            else:
                shipment_data = self.pipeline.acu_api.get_package_details(shipment_data, 'put')
            bp = 'here'



    def load_receipts(self, data_transformed):
        '''`load_receipts`(self, data_transformed)
        ---
        <hr>
        
        Used by CreateAcuReceipt, Iterates through each shipment needing a receipt, determines if it's ready to be created and acts accordingly
        
        <hr>
        
        Parameters
        ----------
        
        :param data_transformed: _description_
        :type data_transformed: _type_
        
        
        '''
        data_loaded = []
        for order in data_transformed:
            shipment_data = self.pipeline.acu_api.sales_order_get_shipment(order)
            if shipment_data['ShipmentNbr']:
                receipt_response = self.pipeline.acu_api.shipment_details(shipment_data)

                for line in receipt_response['details']:
                    line = self.check_reason_code(receipt_response, line)
                if receipt_response['package_count'] == 0 or receipt_response['package_count'] != receipt_response['line_count']:
                    receipt_response = self.pipeline.acu_api.add_package(receipt_response)
                else:
                    receipt_response = self.pipeline.acu_api.get_package_details(receipt_response, 'put')
                self.check_if_ready_for_confirm(receipt_response)
            else:
                self.pipeline.acu_api.order_create_receipt(order)
                self.logger.info(f'Waiting five seconds to give Shipment time to create...')
                time.sleep(5)
                shipment_data = self.pipeline.acu_api.sales_order_get_shipment(order)
                receipt_response = self.pipeline.acu_api.shipment_details(shipment_data)
                for line in receipt_response['details']:
                    line = self.check_reason_code(receipt_response, line)
                receipt_response = self.pipeline.acu_api.add_package(receipt_response)
                self.check_if_ready_for_confirm(receipt_response)
                # self.pipeline.acu_api.confirm_shipment(receipt_response)
        bp = 'here'
    
    def check_reason_code(self, receipt_response, line):
        if line['ReasonCode'] != 'RETURN':
            line = self.pipeline.acu_api.update_reason_code(receipt_response, line)
        return line
    


    def check_package(self, receipt_response: dict):
        '''`check_package`(self, receipt_response: *dict*, )
        ---
        <hr>
        
        Iterates through each Line on the Shipment and aggregates the Qty per item

        Iterates through each Package, and each line on each package, and aggregates the Qty per item

        Compares the Line Items and their respective Qty value against the Package Items
            
        <hr>
        
        Parameters
        ---
        :param (*dict*) `receipt_response`: Receipt data from Acumatica
        
        <hr>
        
        Returns
        ---
        :return `qty_match` (*bool*): Returns **True** if all Items and Quantities match, **False** if not
        '''
        lines = receipt_response['line_count']
        packages = receipt_response['package_count']

        line_detail = {}
        for line in receipt_response['details']:
            if line_detail.get(line['InventoryCD']):
                same_item = line_detail[line['InventoryCD']]
                line_detail[line['InventoryCD']] = {
                    'Qty': int(same_item['Qty'] + line['Qty'])
                }                
            else:
                line_detail[line['InventoryCD']] = {
                    'Qty': int(line['Qty'])
                }

        pkg_detail = {}
        for package in receipt_response['Packages']:
            for p_line in package['PackageContents']:
                item = p_line['InventoryID']['value']
                qty = int(p_line['Quantity']['value'])
                if pkg_detail.get(item):
                    same_item = pkg_detail[item]
                    pkg_detail[item] = {
                        'Qty': same_item['Qty'] + qty
                    }                
                else:
                    pkg_detail[item] = {
                        'Qty': qty
                    }

        qty_match = False
        for key, line_qty in line_detail.items():
            self.logger.info(f'Line: {key} - {line_qty['Qty']} units')
            if pkg_detail.get(key):
                qty_match = line_qty['Qty'] == pkg_detail[key]['Qty']
                self.logger.info(f'On Package: {key} - {pkg_detail[key]['Qty']} units')
            else:
                qty_match = False
        
        bp = 'here'
        return qty_match



    def check_if_ready_for_confirm(self, receipt_response):
        self.logger.info(f'Checking if Shipment {receipt_response['ShipmentNbr']} is ready to Confirm')
        ready = True
        self.logger.info(f'Check 1...Verify Shipment Status is Open...')
        ready = receipt_response['Status'] == 'Open'
        if ready:
            self.logger.info(f'Check 1 Passed. {receipt_response['ShipmentNbr']} is {receipt_response['Status']}')
        else:
            self.logger.error(f'Check 1 Failed! {receipt_response['ShipmentNbr']} is {receipt_response['Status']}')
            return

        self.logger.info(f'Check 2...')
        ready = self.check_package(receipt_response=receipt_response)
        if ready:
            self.logger.info('Check 2 passed!')
            self.pipeline.acu_api.confirm_shipment(receipt_response)
            return
        else:
            self.logger.error('Check 2 Failed!')
            return
