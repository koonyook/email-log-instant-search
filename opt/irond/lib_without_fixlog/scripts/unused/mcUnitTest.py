import memcache

MCconnection = "10.20.4.26:11211"

mc = memcache.Client([MCconnection], debug=0)

print mc.get("2014-01-22-ironport"+"/MID/"+"10.20.4.111:370268719")

