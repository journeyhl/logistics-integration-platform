from pipelines import Pipeline, SendToRMI


rmi = SendToRMI()

result = rmi.run()

bp = 'here'