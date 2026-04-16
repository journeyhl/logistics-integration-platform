import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import SalesOrderCleaner

sales_order_cleaner = SalesOrderCleaner()

completed_sales_order_cleaner = sales_order_cleaner.run()

bp = 'here'
