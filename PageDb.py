
import PageFile
import struct
import zlib
import json
import re
import os
import uuid


class PDTable(object):
	def __init__(self):
		self.name = ''
		self.uuid = uuid.uuid4()


def isstr(s):
	if isinstance(s, str) or isinstance(s, unicode):
		return True
	return False


class PDSuper(object):
	def __init__(self):
		self.version = 1
		self.uuid = uuid.uuid4()
		self.tables = {}
	
	def deserialize(self, s):
		# check min size
		slen = len(s)
		if slen < 10:
			return False
		if s[:6] != 'PAGEDB':
			return False

		# input crc at tail
		crc_str = s[slen-4:]
		crc_in = struct.unpack('<I', crc_str)

		# verify checksum
		data_str = s[:slen-4]
		crc = zlib.crc32(data_str) & 0xffffffff
		if crc != crc_in:
			return False

		try:
			jv = json.loads(data_str[6:])
		except ValueError:
			return False

		if (not isinstance(jv, dict) or
		    'uuid' not in jv or
		    not isstr(jv['uuid']) or
		    'version' not in jv or
		    not isinstance(jv['version'], int) or
		    'tables' not in jv or
		    not isinstance(jv['tables'], dict)):
			return False

		self.version = jv['version']
		if self.version > 1:
			return False

		try:
			self.uuid = uuid.UUID(jv['uuid'])
		except ValueError:
			return False

		for table_k, table_v in jv['tables'].iteritems():
			if (not isinstance(table_v, dict) or
			    not isstr(table_k) or
			    'uuid' not in table_v or
			    not isstr(table_v['uuid'])):
				return False

			m = re.search('^\w+$', table_k)
			if m is None:
				return False

			table = PDTable()
			table.name = table_k
			try:
				table.uuid = uuid.UUID(table_v['uuid'])
			except ValueError:
				return False

		return True
	
	def serialize(self):
		jv = {}
		jv['version'] = self.version
		jv['uuid'] = self.uuid.hex

		jtables = {}

		for pdtable in self.tables:
			jtables[pdtable.name] = { 'uuid' : pdtable.uuid.hex }

		jv['tables'] = jtables

		# magic header, json data
		r = 'PAGEDB'
		r += json.dumps(jv)

		# checksum footer
		crc = zlib.crc32(r) & 0xffffffff
		r += struct.pack('<I', crc)

		return r


class PageDb(object):
	def __init__(self):
		self.dbdir = None
		self.super = None
	
	def open(self, dbdir, readonly=False):
		try:
			fd = os.open(dbdir + '/super', os.O_RDONLY)
			fdata = os.read(fd, 16 * 1024 * 1024)
			os.close(fd)
		except OSError:
			return False

		self.super = PDSuper()
		if not self.super.deserialize(fdata):
			return False

		return True

