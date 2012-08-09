

import os
import zlib
import struct


def isstr(s):
	if isinstance(s, str) or isinstance(s, unicode):
		return True
	return False

def updcrc(data, crc):
	return zlib.crc32(data, crc) & 0xffffffff

def crcheader(s):
	if len(s) < 4:
		return None
	hdr = s[:-4]

	crc_str = s[-4:]
	crc_in = struct.unpack('<I', crc_str)[0]

	crc = zlib.crc32(hdr) & 0xffffffff
	if crc != crc_in:
		return None

	return hdr

def tryread(fd, n):
	try:
		data = os.read(fd, n)
	except OSError:
		return None
	if len(data) != n:
		return None
	return data

def trywrite(fd, data):
	try:
		bytes = os.write(fd, data)
	except OSError:
		return False
	if bytes != len(data):
		return False
	return True

def writeobj(fd, obj):
	data = obj.serialize()
	return trywrite(fd, data)

