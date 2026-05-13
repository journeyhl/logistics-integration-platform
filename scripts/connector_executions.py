import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from connectors import Teams, HubSpotAPI, Sharepoint
from pipelines import HubSpotSnapshot


sp = Sharepoint('testing')
test = sp.get_file('/sites/Marketing/Shared Documents/Ad Planning/Ad Plan 2026.xlsx')
bp = 'here'




hubsnap = HubSpotSnapshot()

properties = hubsnap.hubapi.get_properties('contacts')


hubsnap.centralstore.checked_upsert_paginated('hs.Properties', properties)

bp = 'here'
hubsnap.run()
teams = Teams('script')
bp = teams.send_message('test')
bp = 'here'
# files.list_files(files)



# for p in hubapi.get_deal_pipelines():
#     print(p['id'], p['label'])
#     for s in p.get('stages', []):
#         print(' ', s['id'], s['label'])
# for owner_id, name in api.list_owners().items():
#     print(owner_id, name)