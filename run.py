from pipelines import Pipeline, SendToRMI, FormatData


# rmi = SendToRMI()
rmi = FormatData('sql/SendToRMI.sql')
result = rmi.run()

bp = 'here'