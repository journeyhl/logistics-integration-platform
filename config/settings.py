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
    }
}