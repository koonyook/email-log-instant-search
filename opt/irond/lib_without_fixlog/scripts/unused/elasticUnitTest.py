import elasticsearch

ESconnection = "10.20.4.26:9200"

es = elasticsearch.Elasticsearch(ESconnection)

body=[]
action={
	"update":{
		"_id":"10.20.4.111:4511434",
		"_routing":"4511434",
		"_type":"mnode",
		"_index":"2014-02-10-ironport",
		"_retry_on_conflict":3
	}
}
option={
	"script":"ctx._source.aaa=abc; ",
	"params":{
		"abc":"abc"
	}
}
body.append(action)
body.append(option)
#result=es.bulk(body=body)

result=es.get(index='2014-02-27-ironport',id="10.203.4.131:350941867",doc_type="icnode",refresh=True)
#result=es.get(index='2014-02-26-ironport',id="10.20.4.131:350440748",doc_type="icnode",refresh=True)

print result

'''
try:
	x=es.get(index='nt',id='iii',doc_type="round",refresh="true",routing="1qaz")
	print x
except elasticsearch.exceptions.NotFoundError:
	print "not found"
'''
'''
client=elasticsearch.client.IndicesClient(es)
client.refresh('nt')
getResult=es.search(index='nt',doc_type='round',size=1,body={
	"query":{
		"constant_score": {
			"filter" : {
				"term" : {"_id": 'iii'}    
			}	
		}
	}
})

print getResult
'''
'''
result=es.update(index='nt',doc_type='round',id='333',refresh=True,routing=None,body={
	"script" : "ctx._source.data=v; ",
	"params" : {'v':'zzz'}
})

print result
'''
'''
rawline="""{"action":"update", "indexName":"2014-01-20-ironport", "type":"mnode", "_id":"10.20.4.111:369144551", "value":{"subject":"Welcome to HeroRanger\'s member"}}"""
import json
line=json.loads(rawline)
'''
