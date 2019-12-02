import MySQLdb
import elasticsearch
import json
import os,sys
import math
import subprocess,shlex
import datetime
from datetime import date,timedelta

import smtplib
from email.mime.text import MIMEText

#setting
ESconnection = "?.?.?.?:9200"
SQLconnection = "?.?.?.?"
username = "?"
password = "?"
DBname = "mailx"

#fromAddress = "ins@csloxinfo.net"
#toAddresses = ["ins@csloxinfo.net"]
#ccAddresses = ["c1","c2"]
#subject = "SPAM Blocker Notification"

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

def updateOldSpammer(con,today,authUser,lastCount,status,blockType):
	cur=con.cursor()
	cur.execute("UPDATE blocklog SET lastCount='%s',status='%s' WHERE date='%s' AND authUser='%s'"%(str(lastCount),status,today,authUser))

today=str(datetime.date.today())
con=MySQLdb.connect(SQLconnection,username,password,DBname)

cur=con.cursor()
cur.execute("DELETE FROM blocklog WHERE 1=1")

con.close()
