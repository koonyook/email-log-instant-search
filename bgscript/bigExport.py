#!/usr/bin/python

import MySQLdb
import elasticsearch
import json
import os,sys
import math
import subprocess,shlex
import datetime
from datetime import date,timedelta

#setting
ESconnection = "?.?.?.?:9200"
SQLconnection = "?.?.?.?"
username = "?"
password = "?"
DBname = "mailx"

sectionSize=10000
base="/var/www/html/finishedCSV"

head=['timestamp','sourceIP','authenUser','finishStatus','from','to','size','antivirus','spam']

def writeToFile(result,file):

	for ahit in result['hits']['hits']:
		tmp=ahit['_source']
		row=[]
		for columnName in head:
			if columnName in tmp:
				item='-'
				if isinstance(tmp[columnName],list):
					item='|'.join(tmp[columnName])
				else:
					item=str(tmp[columnName])
				
				if item!='':
					row.append(item.replace( ',', ''))
				else:
					row.append('-')

			else:
				row.append('-')
		file.write(('"'+'","'.join(row)+'"\n').encode('utf-8'))

	return True

def generateDateRange(fromDate,toDate):
	ans=[]
	#verify
	if len(fromDate)!=10 or len(toDate)!=10:
		return []
	else:
		i=0
		iDate=fromDate
		while True:
			ans.append(iDate)
			i+=1
			if i>100 or iDate==toDate:
				break
			else:
				tmp=iDate.split('-')
				iDate=(date(int(tmp[0]),int(tmp[1]),int(tmp[2]))+timedelta(days=1)).isoformat()
	return ans

#create a blank file
#f=open("running","w")
#f.close()

es = elasticsearch.Elasticsearch(ESconnection)
con=MySQLdb.connect(SQLconnection,username,password,DBname)
cur=con.cursor()
cur2=con.cursor()
while True:
	total=cur.execute("SELECT `task_id`, `ownerEmail`, `authenUser`, `fromDate`, `toDate`, `sourceIP`, `from`, `to`, `subject` FROM bigtask WHERE `status`='inqueue' ORDER BY requestTimestamp ASC")
	#total=cur.rowcount
	print total
	if total<=0:
		break
	#print total
	for i in range(total):
		row=cur.fetchone()
		task_id=row[0]
		ownerEmail=row[1]
		authenUser=row[2]
		fromDate=row[3]
		toDate=row[4]
		sourceIP=row[5]
		fromEmail=row[6]
		toEmail=row[7]
		subject=row[8]
		
		command="mkdir -p "+base+'/'+ownerEmail
		subprocess.Popen(shlex.split(command))
		command="chmod +x "+base+'/'+ownerEmail
		subprocess.Popen(shlex.split(command))
		
		outFile=open(base+'/'+ownerEmail+'/'+str(task_id)+'.csv','w')
		outFile.write(','.join(head)+'\n')
		
		cur2.execute("UPDATE bigtask SET status='processing' WHERE task_id=%d"%task_id)

		#generate elasticsearch query
		body={}
		if sourceIP=='*' and authenUser=='*' and fromEmail=='*' and toEmail=='*' and subject=='*':
			body['query']={"match_all":{}}
		else:
			wildcardList=[]
			if sourceIP!='*':
				wildcardList.append({"wildcard":{"sourceIP":sourceIP}})
			if authenUser!='*':
				wildcardList.append({"wildcard":{"authenUser":authenUser}})
			if fromEmail!='*':
				wildcardList.append({"wildcard":{"from":fromEmail}})
			if toEmail!='*':
				wildcardList.append({"wildcard":{"to":toEmail}})
			if subject!='*':
				wildcardList.append({"wildcard":{"subject":subject}})
			body['query']={"bool":{"must":wildcardList}}
		
		message=''
		for iDate in generateDateRange(fromDate,toDate):
			try:
				print iDate
				#count first
				countResult=es.count(index=iDate+'-ironport',doc_type='mnode',body=body['query'])
				#print body
				print countResult
				totalCount=countResult['count']
				print "totalCount",totalCount
				message+=iDate+' = '+str(totalCount)+' records</br>'
				for j in range(int(math.ceil(float(totalCount)/sectionSize))):
					print "j",j
					body['from']=j*sectionSize
					result=es.search(index=iDate+'-ironport',doc_type='mnode',sort="timestamp:asc",size=sectionSize,body=body)
					writeToFile(result,outFile)
			except:
				print sys.exc_info()
				if 'IndexMissingException' in sys.exc_info()[1][1]:
					message+=iDate+' = Index is not kept.</br>'
				else:
					message+=iDate+' = ERROR</br>'+str(sys.exc_info()[1])+'</br>'		

		outFile.close()
				
		cur2.execute("UPDATE bigtask SET status='done', message='%s', finishTimestamp='%s' WHERE task_id=%d"%(message.replace("'",''),str(datetime.datetime.now()).split('.')[0],task_id))

#os.remove("running")
