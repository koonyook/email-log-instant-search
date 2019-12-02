import time,datetime
import os
import os.path
import subprocess, shlex

#this script will revive irond service if it die
print "start reviving process for irond..."
print "don't kill me"
while True:
	time.sleep(5)

	if not os.path.exists('/var/run/irond.pid'):
		print str(datetime.datetime.now()),"pid file has been lost!"
		result = subprocess.Popen(shlex.split("service irond start"), stdout=subprocess.PIPE)
		output=result.communicate()
		print output[0]
		print output[1]
		continue
	else:
		# Get the pid from the pidfile
		try:
			pf = file('/var/run/irond.pid','r')
			pid = int(pf.read().strip())
			pf.close()
		except IOError:
			pid = None
			continue

		#check if the process is running
		if not os.path.exists("/proc/"+str(pid)):
			print str(datetime.datetime.now()),"process has ben lost!"
			result = subprocess.Popen(shlex.split("service irond restart"), stdout=subprocess.PIPE)
			output=result.communicate()
			print output[0]
			print output[1]
		else:
			pass


