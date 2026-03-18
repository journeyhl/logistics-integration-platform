import requests
from config.settings import RMI
import json
import xmltodict
from datetime import datetime
import logging

class RMIXMLConnector:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.rmi_xml')
        self.login()
        self.send_url = 'https://jhl.returnsmanagement.com/webserviceV2/rma/rmaservice.asmx'
        self.send_headers = {
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction': 'http://bactracs.bactracksrl.com/rmaservice/CreateNew'
        }
        self.results = []
        pass

    def login(self):
        login_url = 'https://jhl.returnsmanagement.com/webserviceV2/user/Authentication.asmx'
        login_headers = {
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction': 'http://bactracs.bactracksrl.com/user/Login'
        }
        login_body = f'''<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <Login xmlns="http://bactracs.bactracksrl.com/user">
      <sUserName>{RMI['username']}</sUserName>
      <sPassword>{RMI['password']}</sPassword>
    </Login>
  </soap12:Body>
</soap12:Envelope>'''
        response = requests.post(login_url, data=login_body, headers=login_headers)
        try:
            xml_response = xmltodict.parse(response.content)
            login_result = xml_response['soap:Envelope']['soap:Body']['LoginResponse']['LoginResult']
            self.session_id = login_result['Message']
            self.login_result = login_result['Result']
        except Exception as e:    
            bp = 'here'
        bp = 'here'


    def initiate_send(self, data_transformed):
        type_w = []
        type_3 = []

        for item, data in data_transformed.items():
            if data[0]['RMAType'] == 'W':
                type_w.append(data)
            elif data[0]['RMAType'] == '3':
                type_3.append(data)

        bp = 'here'
        for shipment in type_w:
            self.post_W(shipment)

        for return_order in type_3:
            self.post_3(return_order)

        bp = 'here'
        self.pipeline.acu_api._logout()
        return self.results





    def post_W(self, shipment):
        line_str = f'1 line' if len(shipment) == 1 else f'{len(shipment)} lines'
        self.logger.info(f'Preparing {shipment[0]['RMANumber']} - {line_str}')
        row_text = self._format_w_lines(shipment)

        send_str = f'''<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <CreateNew xmlns="http://bactracs.bactracksrl.com/rmaservice">
      <sGuid>{self.session_id}</sGuid>
      <NewRMA>
        <RMANumber>{shipment[0]['RMANumber']}</RMANumber>
        <RMATypeName>{shipment[0]['RMAType']}</RMATypeName>
        <ShipMethod>{'LTL' if shipment[0]['ShipVia'] == 'LTL' else shipment[0]['ShipVia']}</ShipMethod>
        <CustomerRef>{shipment[0]['OrderNbr']}</CustomerRef>
        <Customer>
          <ShipTo>
            <CompanyName>{shipment[0]['CompanyName'][:25].replace('&', '&amp;')}</CompanyName>
            <Contact>{shipment[0]['ShipToName'][:25].replace('&', '&amp;')}</Contact>
            <ContactEmail>{shipment[0]['ShipToEmailContact']}</ContactEmail>
            <Address1>{shipment[0]['ShipToAddress1']}</Address1>
            <Address2>{shipment[0]['ShipToAddress2']}</Address2>
            <City>{shipment[0]['ShipToCity']}</City>
            <State>{shipment[0]['ShipToState']}</State>
            <Zip>{shipment[0]['ShipToZip']}</Zip>
            <Phone>{shipment[0]['ShipToPhone']}</Phone>
            <Country>{shipment[0]['ShipToCountry']}</Country>
          </ShipTo>
        </Customer>
        <RMALines>{row_text}
        </RMALines>
      </NewRMA>
    </CreateNew>
  </soap:Body>
</soap:Envelope>'''
        # print(send_str)
        acu_response, acu_payload = None, None
        try:
          rmi_response = requests.post(self.send_url, data=send_str, headers=self.send_headers)
          if rmi_response:
              self.get_rmi_msg(rmi_response)
              bp ='here'
          else:
              self.rmi_response_str = f'{shipment[0]['RMANumber']} - CRITICAL ERROR: No response from RMI!'
              self.logger.error(self.rmi_response_str)
              print(send_str)
          if 'error' in self.rmi_response_str.lower():
              acu_response = f'{shipment[0]['RMANumber']} - RMI Error, did not attempt SendToWH in Acu'
              self.logger.warning(acu_response)
              acu_payload = ''
          else:              
              acu_response, acu_payload = self.pipeline.acu_api.sent_to_wh(shipment[0]['RMANumber'], shipment[0]['CustomerID'])
          info = {
              'key': shipment[0]['RMANumber'],
              'lines': len(shipment),
              'rmi_response': self.rmi_response_str,
              'rmi_payload': send_str,
              'acu_response': acu_response,
              'acu_payload': acu_payload,
              'shipment_data': shipment,
              'timestamp': datetime.now()
          }
          
        except Exception as e:
          info = {
              'key': shipment[0]['RMANumber'],
              'lines': len(shipment),
              'rmi_response': self.rmi_response_str,
              'rmi_payload': send_str,
              'acu_response': acu_response,
              'acu_payload': acu_payload,
              'shipment_data': shipment,
              'timestamp': datetime.now()
          }
          bp = 'here'
        self.results.append(info)
        return info


    def get_rmi_msg(self, rmi_response: requests.Response):
        status_code = rmi_response.status_code
        try:
            self.rmi_response_info = xmltodict.parse(rmi_response.text)['soap:Envelope']['soap:Body']['CreateNewResponse']['CreateNewResult']
            if self.rmi_response_info.get('RMA_Number') or self.rmi_response_info.get('RMANumber'):        
                rma = self.rmi_response_info.get('RMA_Number') if self.rmi_response_info.get('RMA_Number') else self.rmi_response_info.get('RMANumber')       
                self.rmi_response_str = f'{status_code} {self.rmi_response_info['Message']}: {rma} posted successfully!'
                self.logger.info(self.rmi_response_str)
            else:
                self.rmi_response_str = f'{status_code} ERROR: {self.rmi_response_info['Message']}'
                self.logger.error(self.rmi_response_str)
        except:
            self.rmi_response_info = {'Result': 'false', 'Message': 'Error parsing response'}
            self.rmi_response_str = f'{status_code} ERROR: {self.rmi_response_info['Message']}'
            self.logger.error(self.rmi_response_str)


    def post_3(self, return_order):
        line_str = f'1 line' if len(return_order) == 1 else f'{len(return_order)} lines'
        self.logger.info(f'Preparing {return_order[0]['ReturnNbr']} - {line_str}')
        row_text = self._format_3_lines(return_order)

        send_str = f'''<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <CreateNew xmlns="http://bactracs.bactracksrl.com/rmaservice">
      <sGuid>{self.session_id}</sGuid>
      <NewRMA>
        <RMANumber>{return_order[0]['ReturnNbr']}</RMANumber>
        <RMATypeName>{return_order[0]['RMAType']}</RMATypeName>
        <ShipMethod>{'LTL' if return_order[0]['ShipVia'] == 'LTL' else return_order[0]['ShipVia']}</ShipMethod>
        <CustomerRef>{return_order[0]['OriginalOrderNbr']}</CustomerRef>
        <Customer>
          <ShipTo>
            <CompanyName>{return_order[0]['CompanyName']}</CompanyName>
            <Contact>{return_order[0]['ShipToName']}</Contact>
            <ContactEmail>{return_order[0]['ShipToEmailContact']}</ContactEmail>
            <Address1>{return_order[0]['ShipToAddress1']}</Address1>
            <Address2>{return_order[0]['ShipToAddress2']}</Address2>
            <City>{return_order[0]['ShipToCity']}</City>
            <State>{return_order[0]['ShipToState']}</State>
            <Zip>{return_order[0]['ShipToZip']}</Zip>
            <Phone>{return_order[0]['ShipToPhone']}</Phone>
            <Country>{return_order[0]['ShipToCountry']}</Country>
          </ShipTo>
        </Customer>
        <RMALines>{row_text}
        </RMALines>
      </NewRMA>
    </CreateNew>
  </soap:Body>
</soap:Envelope>'''
        # print(send_str)
        msg = ''
        result = ''
        acu_response, acu_payload = None, None
        try:
          rmi_response = requests.post(self.send_url, data=send_str, headers=self.send_headers)
          if rmi_response:
              self.get_rmi_msg(rmi_response)
              bp ='here'
          else:
              self.rmi_response_str = 'CRITICAL ERROR: No response from RMI!'
              self.logger.error(self.rmi_response_str)
          if 'error' in self.rmi_response_str.lower():
              acu_response = 'RMI Error, did not attempt RCSendToWH in Acu'
              self.logger.warning(acu_response)
              acu_payload = ''
          else:
              acu_response, acu_payload = self.pipeline.acu_api.rc_sent_to_wh(return_order[0]['ReturnNbr'],
                                                                          return_order[0]['OrderType'], 
                                                                          return_order[0]['CustomerID'])
          info = {
              'key': return_order[0]['ReturnNbr'],
              'lines': len(return_order),
              'rmi_response': self.rmi_response_str,
              'rmi_payload': send_str,
              'acu_response': acu_response,
              'acu_payload': acu_payload,
              'shipment_data': return_order,
              'timestamp': datetime.now()
          }
          
        except Exception as e:
          info = {
              'key': return_order[0]['ReturnNbr'],
              'lines': len(return_order),
              'rmi_response': self.rmi_response_str,
              'rmi_payload': send_str,
              'acu_response': acu_response,
              'acu_payload': acu_payload,
              'shipment_data': return_order,
              'timestamp': datetime.now()
          }
          bp = 'here'
        self.results.append(info)
        return info



    def _format_w_lines(self, shipment):
        ship_str = ''
        for row in shipment:
            ship_str += f"""
            <RMALine>
              <DFItem>{row['DFPart']}</DFItem>
              <RPItem>{row['RPPart']}</RPItem>
              <RPQuantity>{row['RPQuantity']}</RPQuantity>
              <RPLocation>{row['RPLocation']}</RPLocation>
            </RMALine>"""
        return ship_str
    
    def _format_3_lines(self, shipment):
        ship_str = ''
        for row in shipment:
            ship_str += f"""
            <RMALine>
              <DFItem>{row['DFPart']}</DFItem>
              <DFQuantity>{row['DFQuantity']}</DFQuantity>
              <DFSerialNumbers>{row['SerialNumber']}</DFSerialNumbers>
              <DFCategory></DFCategory>
              <DFComments></DFComments>
            </RMALine>"""
        return ship_str