#!/usr/bin/python

import MySQLdb
import elasticsearch
import json
import os,sys
import math
import subprocess,shlex
import datetime
import ldap
import ldap.modlist as modlist

from datetime import date,timedelta

import smtplib
from email.mime.text import MIMEText

import hashlib,string,random

#setting
ESconnection = "?.?.?.?:9200"
SQLconnection = "?.?.?.?"
username = "?"
password = "?"
DBname = "mailx"

LDAPconnection = "?.?.?.?"
LDAPusername = "cn=?, o=?"
LDAPpassword = "?"

def generateRandomPassword(size):
    allow=string.ascii_letters+string.digits
    password=''
    for i in range(size):
        password+=allow[random.randint(0,len(allow)-1)]
    return password

def makeSecret(password):
    salt=os.urandom(4)
    h = hashlib.sha1(password)
    h.update(salt)
    return "{SSHA}"+(h.digest()+salt).encode('base64').strip()

def tryToChangePassword(authUser):
	'''
	if success, this function will return new password
	'''
	#return None
	domain=authUser.rsplit('@')[-1]	
	if domain not in ['loxinfra.co.th','abccoms.com','abcinfra.com']:
		return None

	try:
		l=ldap.open(LDAPconnection)
		l.protocol_version = ldap.VERSION2
		l.simple_bind(LDAPusername, LDAPpassword)

		baseDN = "o=virtual"
		searchScope = ldap.SCOPE_SUBTREE
		retrieveAttributes = None
		searchFilter = "mail=%s"%(authUser,)

		ldap_result_id = l.search(baseDN, searchScope, searchFilter, retrieveAttributes)
		result_type, result_data = l.result(ldap_result_id, 0)
		if result_data==[]:
			#user is not found
			l.unbind()
			return None
		else:
			#user is found
			dn = result_data[0][0]
			oldValue=result_data[0][1]['userPassword'][0]
			newPassword=generateRandomPassword(10)
			newValue=makeSecret(newPassword)
			old = {'userPassword':oldValue}
			new = {'userPassword':newValue}
			ldif = modlist.modifyModlist(old,new)
			l.modify(dn,ldif)
			l.unbind()
			return newPassword

	except ldap.LDAPError, e:
		return None


def sendmail(fromAddress,toAddresses,subject,message):
	#toAddresses must be list
	msg=MIMEText(message)
	msg['Subject']=subject
	msg['From']=fromAddress
	msg['To']=','.join(toAddresses)
	s=smtplib.SMTP('localhost')
	s.sendmail(fromAddress,toAddresses, msg.as_string())
	s.quit()

def getSpammer(today,threshold):
	answer={}
	es = elasticsearch.Elasticsearch(ESconnection)
	body='''{
		"query": {
			"bool": {
			   "must": [
					{"term" : {"spam": "positive"}},
					{"term" : {"isTop": true}},
					{"term" : {"authenResult": "succeeded"}}
			   ]
			}
		},
		"facets":{
			"histo1":{
				"terms": {
				   "field": "authenUser",
				   "size": 100
				}
			}
		}
    }'''
	result=es.search(index=today+'-ironport',doc_type='mnode',body=body)
	for element in result['facets']['histo1']['terms']:
		if element['count']>threshold:
			answer[element['term']]=element['count']
		
	return answer

def getOldSpammerFromDB(con,today):
	cur=con.cursor()
	cur.execute("SELECT authUser, lastCount, status FROM blocklog WHERE date='%s'"%today)
	rows=cur.fetchall()
	count={}
	status={}
	for element in rows:
		count[element[0]]=element[1]
		status[element[0]]=element[2]
	return count,status

def insertNewSpammer(con,today,authUser,lastCount,status,blockType):
	cur=con.cursor()
	cur.execute("INSERT INTO blocklog (date,authUser,lastCount,status,blockType) VALUES ('%s','%s','%s','%s','%s')"%(today,authUser,str(lastCount),status,blockType))

def updateOldSpammer(con,today,authUser,lastCount,status,blockType=None):
	cur=con.cursor()
	if blockType==None:
		cur.execute("UPDATE blocklog SET lastCount='%s',status='%s' WHERE date='%s' AND authUser='%s'"%(str(lastCount),status,today,authUser))
	else:
		cur.execute("UPDATE blocklog SET lastCount='%s',status='%s',blockType='%s' WHERE date='%s' AND authUser='%s'"%(str(lastCount),status,blockType,today,authUser))

today=str(datetime.date.today())
con=MySQLdb.connect(SQLconnection,username,password,DBname)
oldCount,oldStatus=getOldSpammerFromDB(con,today)
newCount=getSpammer(today,10)

emailFrom="ins@abcinfra.net"
#this is for every case
emailTo=["ins@abcinfra.net"]
emailSubject="SPAMMER Notification (%s)"
manualPattern="This user are pending to be blocked manually.\n\n%s\n\nAfter blocking this user manually, please go to http://10.203.30.28/index.php/spamsystem/spamblocker to save your action.\n"
autoPattern="This user was blocked automatically by the system.\n\n%s\n\nFor more information, please go to http://10.203.30.28/index.php/spamsystem/spamblocker/%s\n"

#this is for service (in the case of auto only)
service_emailFrom="ins@abcinfra.net"
service_emailTo=["ts@abcinfra.net","support@abcinfra.com"]
service_emailSubject="Email SPAM Detect from account %s"
service_pattern="Dear All,\n\n    Email SPAM Detect from account: %s\n\n    Password has been changed.\n\n    *** DO NOT USE OLD PASSWORD ***\n\nBest Regards,\n"

#haveToSend=False
for authUser in newCount:
	if authUser not in oldCount:
		#this is new spammer
		newPassword=tryToChangePassword(authUser)
		if newPassword==None:
			insertNewSpammer(con,today,authUser,newCount[authUser],"pending","manual")
			emailContent="NEW SPAMMER: %s (%d spams)\n"%(authUser,newCount[authUser])
			sendmail(emailFrom,emailTo,emailSubject%(authUser,),manualPattern%(emailContent,))
		else:
			insertNewSpammer(con,today,authUser,newCount[authUser],"blocked","auto")
			emailContent="NEW SPAMMER: %s (%d spams)\nThe password was changed to %s"%(authUser,newCount[authUser],newPassword)
			sendmail(emailFrom,emailTo,emailSubject%(authUser,),autoPattern%(emailContent,today))
			sendmail(service_emailFrom,service_emailTo,service_emailSubject%(authUser,),service_pattern%(authUser,))

	elif newCount[authUser]>oldCount[authUser]:
		#old data but have to update
		if oldStatus[authUser]=="pending":
			updateOldSpammer(con,today,authUser,newCount[authUser],"pending")
		elif oldStatus[authUser]=="blocked":
			newPassword=tryToChangePassword(authUser)
			if newPassword==None:
				updateOldSpammer(con,today,authUser,newCount[authUser],"pending","manual")
				emailContent="MORE SPAM AFTER BLOCKING: %s (+%d spams)\n"%(authUser,newCount[authUser]-oldCount[authUser])
				sendmail(emailFrom,emailTo,emailSubject%(authUser,),manualPattern%(emailContent,))
			else:
				updateOldSpammer(con,today,authUser,newCount[authUser],"blocked","auto")
				emailContent="MORE SPAM AFTER BLOCKING: %s (+%d spams)\nThe password was changed to %s"%(authUser,newCount[authUser]-oldCount[authUser],newPassword)
				sendmail(emailFrom,emailTo,emailSubject%(authUser,),autoPattern%(emailContent,today))
				sendmail(service_emailFrom,service_emailTo,service_emailSubject%(authUser,),service_pattern%(authUser,))
	else:
		pass

con.close()
#if haveToSend:
#	emailContent+="\r\nAfter blocking them manually, please go to http://10.203.30.28/index.php/spamsystem/spamblocker to save your action.\r\n "
#	sendmail("ins@abcinfra.net",["ins@abcinfra.net"],"SPAMMER Notification (beta test)",emailContent)
