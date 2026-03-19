import logging
from connectors.acu_api import AcumaticaAPI
import time
class Load:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.acu_api: AcumaticaAPI = pipeline.acu_api
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')


    def load(self, data_transformed):        
        data_loaded = []
        for order in data_transformed:
            shipment_data = self.acu_api.sales_order_get_shipment(order)
            if shipment_data['ShipmentNbr']:
                receipt_response = self.acu_api.shipment_details(shipment_data)

                for line in receipt_response['details']:
                    line = self.check_reason_code(receipt_response, line)
                if receipt_response['package_count'] == 0 or receipt_response['package_count'] != receipt_response['line_count']:
                    receipt_response = self.acu_api.add_package(receipt_response)
                else:
                    receipt_response = self.acu_api.get_package_details(receipt_response)
                self.check_if_ready_for_confirm(receipt_response)
            else:
                self.acu_api.sales_order_create_receipt(order)
                self.logger.info(f'Waiting five seconds to give Shipment time to create...')
                time.sleep(5)
                shipment_data = self.acu_api.sales_order_get_shipment(order)
                receipt_response = self.acu_api.shipment_details(shipment_data)
                for line in receipt_response['details']:
                    line = self.check_reason_code(receipt_response, line)
                receipt_response = self.acu_api.add_package(receipt_response)
                self.check_if_ready_for_confirm(receipt_response)
                # self.acu_api.confirm_shipment(receipt_response)
        bp = 'here'
    
    def check_reason_code(self, receipt_response, line):
        if line['ReasonCode'] != 'RETURN':
            line = self.acu_api.update_reason_code(receipt_response, line)
        return line
    

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

        self.logger.info(f'Check 2...Does Package count = Line count?')
        packages = receipt_response['package_count'] 
        pkg_str = f'1 pkg line' if packages == 1 else f'{packages} pkg lines'
        lines = receipt_response['line_count']
        lines_str = f'1 ship line' if lines == 1 else f'{lines} ship lines'
        ready = packages == lines
        if ready:
            self.logger.info(f'Check 2 Passed! {lines_str}, {pkg_str}')
        else:
            self.logger.error(f'Check 2 Failed! {lines_str}, {pkg_str}')
            return
        details_item_comparison = {}
        self.logger.info(f'Check 3...Verifying item(s) on shipment and respective Qty values')
        for d_line in receipt_response['details']:
            item = d_line['InventoryCD']
            qty = int(d_line['Qty'])
            q_string = f'{qty} unit' if qty == 1 else f'{qty} units'
            self.logger.info(f'Line Details...{receipt_response['ShipmentNbr']}-{d_line['LineNbr']}: {item} - {q_string}')
            if details_item_comparison.get(item):
                details_item_comparison[item] = {
                    'Qty': int(qty + details_item_comparison[item]['Qty'])
                }
            else:
                details_item_comparison[item] = {
                    'Qty': qty
                }
        
        package_item_comparison = {}
        for package in receipt_response['Packages']:
            for p_line in package['PackageContents']:
                item = p_line['InventoryID']['value']
                qty = int(p_line['Quantity']['value'])
                q_string = f'{qty} unit' if qty == 1 else f'{qty} units'
                self.logger.info(f'Package Details...{receipt_response['ShipmentNbr']}-{p_line['rowNumber']}: {item} - {q_string}')
                if package_item_comparison.get(item):
                    package_item_comparison[item] = {
                        'Qty': int(qty + package_item_comparison[item]['Qty'])
                    }
                else:
                    package_item_comparison[item] = {
                        'Qty': qty
                    }
        
        bp = 'here'
        ready = details_item_comparison == package_item_comparison
        if ready:
            self.logger.info('Check 3 passed!')
            self.acu_api.confirm_shipment(receipt_response)
            return
        else:
            self.logger.error('Check 2 Failed!')
            return
