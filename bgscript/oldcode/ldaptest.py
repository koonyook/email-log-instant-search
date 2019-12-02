import ldap
import ldap.modlist as modlist

import hashlib
import os
import string
import random
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

#try:
l=ldap.open("?.?.?.?")
l.protocol_version = ldap.VERSION2

username = "cn=?, o=?"
password = "?"

l.simple_bind(username, password)

baseDN = "o=?"
searchScope = ldap.SCOPE_SUBTREE
retrieveAttributes = None
searchFilter = "mail=?@?.?"

ldap_result_id = l.search(baseDN, searchScope, searchFilter, retrieveAttributes)
print ldap_result_id

result_type, result_data = l.result(ldap_result_id, 0)
if result_data==[]:
	#user is not found
	pass
else:
	#user is found

	dn = result_data[0][0]
	oldPassword=result_data[0][1]['userPassword'][0]
	print dn
	print "#####"
	
	#change password
	newPassword=makeSecret('?')
	old = {'userPassword':oldPassword}
	new = {'userPassword':newPassword}
	print old
	print new
	ldif = modlist.modifyModlist(old,new)
	print l.modify(dn,ldif)
	#m.unbind_s()
l.unbind()

print generateRandomPassword(10)
#except ldap.LDAPError, e:
	#print e


