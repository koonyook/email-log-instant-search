#!/usr/bin/python -u

import sys,string,re,time

def log2mysql(f_pattern,line):
    sql = ""
    fp = open(f_pattern,'r')
    patterns = fp.readlines()
    fp.close()
    for pattern in patterns:
        pair = string.split(pattern,'~')
        sql = pair[2]
        m = re.match(pair[1], line)
        if m != None:
            num = 0
            for i in m.groups():
                sql = string.replace(sql,'#S'+str(num)+'#',i)
                if ('%S'+str(num)+'%' in sql):
				    sql = string.replace(sql,'%S'+str(num)+'%',str(int(i,16)%5))
                num = num+1
            return sql

def main(argv = sys.argv):
    f_pattern = '/root/scriptsironport/ironport-production.pattern' 
    line = ''

    if len(argv) == 2: 
        f_pattern = argv[1] 
    elif len(argv) > 2:
        print "Invalid argument !!"
        sys.exit(0)

    if not sys.stdin.isatty():
        #line = sys.stdin.readline().strip()
		line = raw_input()
        #line = sys.stdin.read()
        #print line
        #sys.stdout.flush()
        #line = raw_input() 

    while True:		#line != '': 
        output = log2mysql(f_pattern,line)
        if output != None: 
            try:
                print output,
                sys.stdout.flush()
            except IOError:
                pass

        if not sys.stdin.isatty():
            #line = sys.stdin.readline().strip()
			line = raw_input()
            #line = sys.stdin.read()
            #print line
            #sys.stdout.flush()
            #line = raw_input() 
        #time.sleep(5)
    #sys.exit(0)

if __name__ == "__main__":
    try:
        main(sys.argv)
    except KeyboardInterrupt:
        pass

