
#
# Copyright 2012 Red Hat, Inc.
#
# Distributed under the MIT/X11 software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.
#

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

def writepb(fd, recname, obj):
	if len(recname) != 4:
		return False
	data = obj.SerializeToString()
	msg = recname + struct.pack('<I', len(data)) + data

	crc = updcrc(msg, 0)
	crc_str = struct.pack('<I', crc)

	msg += crc_str

	return trywrite(fd, msg)

def readrec(fd):
	hdr = tryread(fd, 8)
	if hdr is None:
		return None

	recname = hdr[:4]
	datalen = struct.unpack('<I', hdr[4:])[0]

	if datalen > (16 * 1024 * 1024):
		return None

	data = tryread(fd, datalen)
	crc_str = tryread(fd, 4)
	if data is None or crc_str is None:
		return None
	crc_in = struct.unpack('<I', crc_str)[0]

	crc = updcrc(hdr, 0)
	crc = updcrc(data, crc)

	if crc != crc_in:
		return None

	return (recname, data)

def readrecstr(s):
	if len(s) < 8:
		return None
	hdr = s[:8]
	pos = 8

	recname = hdr[:4]
	datalen = struct.unpack('<I', hdr[4:])[0]

	if datalen > (16 * 1024 * 1024):
		return None

	if len(s) < pos + datalen + 4:
		return None
	data = s[pos:pos+datalen]
	pos += datalen

	crc_str = s[pos:pos+4]
	crc_in = struct.unpack('<I', crc_str)[0]

	crc = updcrc(hdr, 0)
	crc = updcrc(data, crc)

	if crc != crc_in:
		return None

	return (recname, data)

