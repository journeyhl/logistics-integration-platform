import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import AcuToDbcSalesOrders



sales_orders_to_dbc = AcuToDbcSalesOrders()
completed_sales_orders_to_dbc = sales_orders_to_dbc.run()

bp = 'here'