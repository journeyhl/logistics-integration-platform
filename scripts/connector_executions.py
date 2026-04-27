import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from connectors import Teams


teams = Teams('script')
bp = teams.send_message('test')
bp = 'here'
# files.list_files(files)