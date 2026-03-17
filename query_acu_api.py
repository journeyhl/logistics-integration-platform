


from connectors import AcumaticaAPI

acu = AcumaticaAPI('acu')
customer = acu.customers(query="MainContact/Phone1 eq '(801) 506-1834' or MainContact/Phone2 eq '4194677063'")


contact = acu.contact(query="Phone1 eq '(724) 350-0048' or Phone2 eq '(724) 350-0048'")

bp = 'here'

