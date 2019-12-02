import email
from email.header import decode_header, make_header

def readSubject(message):
	message=message.replace("\r\n","")
	results=decode_header(message)
	answer=u""
	for result in results:
		if result[1]==None or result[1]=='utf-8':
			answer+=unicode(result[0],'utf-8')
		else:
			#answer+=unicode(result[0],result[1])
			answer+=unicode(result[0],'cp874')
	
	return answer.lower()

message2="""=?UTF-8?Q?=E0=B8=9B=E0=B8=B5=E0=B9=83=E0=B8=AB?=\r\n =?UTF-8?Q?=E0=B8=A1=E0=B9=88_=E0=B8=AD=E0=B8=A2?=\r\n =?UTF-8?Q?=E0=B8=B2=E0=B8=81=E0=B9=84=E0=B8=94?=\r\n =?UTF-8?Q?=E0=B9=89=E0=B8=87=E0=B8=B2=E0=B8=99=E0=B9=83?=\r\n =?UTF-8?Q?=E0=B8=AB=E0=B8=A1=E0=B9=88_?=\r\n =?UTF-8?Q?=E0=B8=95=E0=B9=89=E0=B8=AD=E0=B8=87=E0=B9=80?=\r\n =?UTF-8?Q?=E0=B8=A3=E0=B8=B4=E0=B9=88=E0=B8=A1?=\r\n =?UTF-8?Q?=E0=B8=AB=E0=B8=B2=E0=B8=87=E0=B8=B2=E0=B8=99?=\r\n =?UTF-8?Q?=E0=B8=95=E0=B8=B1=E0=B9=89=E0=B8=87?=\r\n =?UTF-8?Q?=E0=B9=81=E0=B8=95=E0=B"""

#message2="""=?UTF-8?Q?=E0=B8=9B=E0=B8=B5=E0=B9=83=E0=B8=AB?=\r\n =?UTF-8?Q?=E0=B8=A1=E0=B9=88_=E0=B8=AD=E0=B8=A2?=\r\n =?UTF-8?Q?=E0=B8=B2=E0=B8=81=E0=B9=84=E0=B8=94?=\r\n =?UTF-8?Q?=E0=B9=89=E0=B8=87=E0=B8=B2=E0=B8=99=E0=B9=83?=\r\n =?UTF-8?Q?=E0=B8=AB=E0=B8=A1=E0=B9=88_?=\r\n =?UTF-8?Q?=E0=B8=95=E0=B9=89=E0=B8=AD=E0=B8=87=E0=B9=80?=\r\n =?UTF-8?Q?=E0=B8=A3=E0=B8=B4=E0=B9=88=E0=B8=A1?=\r\n =?UTF-8?Q?=E0=B8=AB=E0=B8=B2=E0=B8=87=E0=B8=B2=E0=B8=99?=\r\n =?UTF-8?Q?=E0=B8=95=E0=B8=B1=E0=B9=89=E0=B8=87?=\r\n"""

message1="AMENDED PF018 Confirmation Ka***RE: Order by Solaris : Instruction\r\n EQ - 06/02/2014 - 1"
message1="=?windows-874?B?utTF4KfUucq0?="

print readSubject(message1)

print readSubject(message2)

message3=message1.replace("\r\n","")
print decode_header(message3)

"""
subject, encoding = decode_header(message)[0]

if encoding==None:
	print "\n%s (%s)\n"%(subject, encoding)
else:
	print "\n%s (%s)\n"%(subject.decode(encoding), encoding)
"""
