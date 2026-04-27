import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import AcumaticaDeletions
from connectors import SQLConnector, AcumaticaAPI
import json

central = SQLConnector('script', 'db_CentralStore')
acudb = SQLConnector('script', 'AcumaticaDb')
acuapi = AcumaticaAPI('script')

payloads = central.query_db(
query="""
select distinct payload
from _util.acu_api_log a
where a.Response = 'Failure' and cast(timestamp as date) = cast(getdate() as date)
and a.KeyValue not in(
'080711',
''
)
""").to_dicts()

for payload_dict in payloads:
    payload = json.loads(payload_dict['payload'])
    shipment_nbr = payload['ShipmentNbr']['value']
    customer_id = payload['CustomerID']['value']
    print(shipment_nbr)
    get_response = acuapi.session.get(f'{acuapi.base_uri}/Shipment/{shipment_nbr}?$custom=Document.AttributeSHP2WH')
    current_sent_to_wh = get_response.json().get('custom', {}).get('Document', {}).get('AttributeSHP2WH', {}).get('value')
    print(f'current SentToWH: {current_sent_to_wh}')
    if not current_sent_to_wh:
        print('sending')
        response = acuapi.session.put(f'{acuapi.base_uri}/Shipment', json = payload)
        print(response)
        bp = 'here'
    else:
        bp = 'here'
    bp = 'here'
bp = 'here'
