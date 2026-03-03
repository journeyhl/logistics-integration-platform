from pipelines import Pipeline, SendShipments, FormatData


rmi = SendShipments()
# rmi = FormatData('sql/SendToRMI.sql')
result = rmi.run()

bp = 'here'