import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from connectors import Teams, HubSpotAPI
from pipelines import HubSpotSnapshot


# for p in hubapi.get_deal_pipelines():
#     print(p['id'], p['label'])
#     for s in p.get('stages', []):
#         print(' ', s['id'], s['label'])
# for owner_id, name in api.list_owners().items():
#     print(owner_id, name)


hubsnap = HubSpotSnapshot()

properties = hubsnap.hubapi.get_properties('contacts')

# for item in properties:
#     for key, value in item.items():
#         print(key)
#     bp = 'here'


# for property in properties

hubsnap.centralstore.checked_upsert_paginated('hs.Properties', properties)

bp = 'here'
hubsnap.run()
teams = Teams('script')
bp = teams.send_message('test')
bp = 'here'
# files.list_files(files)