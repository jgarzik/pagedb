
import PageFile
import struct
import zlib

class PDTable(object):
	def __init__(self):
		self.name = ''
		self.root_idx = -1

class PDSuper(object):
	def __init__(self):
		self.magic = 'PAGEDB'
		self.tables = {}
	
	def deserialize(self, s):
		# check min size
		slen = len(s)
		if slen < 10:
			return False

		# input crc at tail
		crc_str = s[slen-4:]
		crc_in = struct.unpack('<I', crc_str)

		# verify checksum
		data_str = s[:slen-4]
		crc = zlib.crc32(data_str) & 0xffffffff
		if crc != crc_in:
			return False

		# verify magic header id
		l = data_str.split()
		l.reverse()
		v = l.pop()
		if v != 'PAGEDB':
			return False

		# read table data
		while len(l) > 1:
			table = PDTable()
			table.name = l.pop()
			table.root_idx = int(l.pop())
			if table.root_idx <= 0:
				return False
			self.tables[table.name] = table

		return True
	
	def serialize(self):
		l = ['PAGEDB']
		for table in self.tables.itervalues():
			l.append(table.name)
			l.append(str(table.root_idx))
		r = ' '.join(l)
		crc = zlib.crc32(r) & 0xffffffff
		r += struct.pack('<I', crc)
		return r

class PageDb(object):
	def __init__(self):
		self.pf = None
	
	def open(self, filename, readonly=False):
		self.pf = PageFile.PageFile()
		if not self.pf.open(filename, readonly):
			return False

		if self.pf.n_pages == 0:
			return self.init_sb()
		return self.read_sb()

	def init_sb(self):
		
