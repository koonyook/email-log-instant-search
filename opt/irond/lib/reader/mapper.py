import sys,string,re,time
import setting

def clearQuote(str):
	return str.replace('"',"'")
	#return str.replace(r"\"","\\"+r"\"").replace(r"\'","\\"+r"\'")
	#rawline=rawline.replace("\\", r"\\").replace(r"\"","\\"+r"\"").replace(r"\"}}","\"}}")

def translateToJSON(line,patterns):
	sql = ""
	for pattern in patterns:
		pair = string.split(pattern,'~')
		sql = pair[2]
		m = re.match(pair[1], line)
		if m != None:
			num = 0
			for i in m.groups():
				sql = string.replace(sql,'#S'+str(num)+'#',clearQuote(i))
				if ('%S'+str(num)+'%' in sql):
					sql = string.replace(sql,'%S'+str(num)+'%',str(int(i,16)%5))
				num = num+1
			return sql
	return None


