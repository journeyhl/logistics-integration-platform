import os
from dotenv import load_dotenv

load_dotenv()

# Database configurations as dictionaries
DATABASES = {
    'db_CentralStore': {
        'server': os.getenv('CENTRALSTORE_SERVER'),
        'database': os.getenv('CENTRALSTORE_DATABASE'),
        'username': os.getenv('CENTRALSTORE_USERNAME'),
        'password': os.getenv('CENTRALSTORE_PASSWORD'),
    },
    'AcumaticaDb': {
        'server': os.getenv('ACUMATICA_SERVER'),
        'database': os.getenv('ACUMATICA_DATABASE'),
        'username': os.getenv('ACUMATICA_USERNAME'),
        'password': os.getenv('ACUMATICA_PASSWORD'),
    }
}

RMI = {
    'username': os.getenv('RMI_USERNAME'),
    'password': os.getenv('RMI_PASSWORD')
}

REDSTAG = {
    'username': os.getenv('REDSTAG_USERNAME'),
    'password': os.getenv('REDSTAG_PASSWORD')
}

ACUMATICA_API = {
    'username': os.getenv('ACUMATICA_API_USERNAME'),
    'password': os.getenv('ACUMATICA_API_PASSWORD')
}


TABLES = {
    'rmi_Receipts':{
        'keys': ['RMANumber', 'ReceiptID', 'RMAID', 'RMALineID'],
        'columns': [
            'RMANumber',
            'ReceiptDate',
            'ReceiptID',
            'RMAID',
            'RMALineID',
            'Qty',
            'InventoryCD',
            'Location',
            'ItemType',
            'ItemCategory',
            'Descr',
            'Price',
            'Cost'
        ],
        'update_columns':['ReceiptDate', 'Qty', 'InventoryCD', 'Location', 'ItemType', 'ItemCategory', 'Descr', 'Price', 'Cost']
    },    
    'rmi_ClosedShipments':{
        'keys': ['RMANumber', 'RMAID', 'RMALineID', 'RMAType'],
        'columns': [
            'RMANumber',
            'RMAID',
            'RMALineID',
            'RMAType',
            'CreateDate',
            'ShipDate',
            'InventoryCD',
            'QtyShipped',
            'QtyToShip',
            'Location',
            'ItemCategory',
            'Descr',
            'Carrier',
            'CarrierCode',
            'Priority',
            'Tracking',
            'FreightCost',
            'OutboundShipMethod'
        ],
        'update_columns':[
            'CreateDate',
            'ShipDate',
            'InventoryCD',
            'QtyShipped',
            'QtyToShip',
            'Location',
            'ItemCategory',
            'Descr',
            'Carrier',
            'CarrierCode',
            'Priority',
            'Tracking',
            'FreightCost',
            'OutboundShipMethod'
        ]
    }, 
    'rmi_RMAStatus':{
        'keys': ['RMANumber', 'RMAID', 'RMALineID', 'RMAType'],
        'columns': [
            'RMANumber',
            'RMAID',
            'RMALineID',
            'RMAType',
            'RMAStatus',
            'CustomerRef',
            'RMALineNbr',
            'DFStatus',
            'InventoryCD',
            'Qty',
            'Descr',
            'RMATypeName',
            'CreateDate',
            'RMILastModifiedDate',
            'LastChecked'
        ],
        'update_columns':[
            'RMAStatus',
            'CustomerRef',
            'RMALineNbr',
            'DFStatus',
            'InventoryCD',
            'Qty',
            'Descr',
            'RMATypeName',
            'CreateDate',
            'RMILastModifiedDate',
            'LastChecked'
        ]
    }, 
    '_util.acu_api_log':{
        'keys': [
            'Entity',
            'KeyValue',
            'Operation',
            'Timestamp'
        ],
        'columns': [
            'Entity',
            'KeyValue',
            'Operation',
            'Payload',
            'Response',
            'Timestamp'
        ],
        'update_columns':[
            'Payload',
            'Response',
        ]
    },
}