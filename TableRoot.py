
import struct
import zlib
import os

from util import tryread


class TableEnt(object):
	def __init__(self):
		self.k = ''
		self.file_id = -1

	def deserialize(self, fd, crc):
		data = tryread(fd, 8)
		if data is None:
			return None
		crc = zlib.crc32(data, crc) & 0xffffffff

		self.file_id, klen = struct.unpack('<II', data)

		self.k = tryread(fd, klen)
		if self.k is None:
			return None
		crc = zlib.crc32(self.k, crc) & 0xffffffff

		return crc

	def serialize(self):
		 r = struct.pack('<II', self.file_id, len(self.k))
		 r += self.k
		 return r


class TableRoot(object):
	def __init__(self, dbdir, root_id):
		self.dbdir = dbdir
		self.root_id = root_id
		self.v = []
		self.dirty = False

	def open(self):
		try:
			name = "%x" % (root_id,)
			fd = os.open(self.dbdir + '/' + name, os.O_RDONLY)
		except OSError:
			self.dirty = True	# does not exist, so perform
			return True		# first-time write

		rc = self.deserialize(fd)

		os.close(fd)

		return rc

	def deserialize(self, fd):
		data = tryread(fd, 4)
		if data != 'ROOT':
			return False

		crc = 0
		crc = zlib.crc32(data, crc) & 0xffffffff

		data = tryread(fd, 4)
		if data is None:
			return False
		n_ent = struct.unpack('<I', data)[0]
		crc = zlib.crc32(data, crc) & 0xffffffff

		for idx in xrange(n_ent):
			ent = TableEnt()
			crc = ent.deserialize(fd, crc)
			if crc is None:
				return False
			v.append(ent)

		crc_str = tryread(fd, 4)
		if crc_str is None:
			return False

		crc_in = struct.unpack('<I', crc_str)[0]
		if crc != crc_in:
			return False

		return True

	def serialize(self, fd):
		data = 'ROOT'
		crc = 0
		crc = zlib.crc32(data, crc) & 0xffffffff
		if not trywrite(fd, data):
			return False

		data = struct.pack('<I', len(self.v))
		crc = zlib.crc32(data, crc) & 0xffffffff
		if not trywrite(fd, data):
			return False

		for ent in self.v:
			data = ent.serialize()
			crc = zlib.crc32(data, crc) & 0xffffffff
			if not trywrite(fd, data):
				return False

		data = struct.pack('<I', crc)
		if not trywrite(fd, data):
			return False

		return True

	def first(self):
		if len(self.v) == 0:
			return None
		return self.v[0]
	
	def last(self):
		if len(self.v) == 0:
			return None
		return self.v[-1]

	def lookup_pos(self, k):
		for idx in xrange(len(self.v)):
			if k <= self.v[idx].k:
				return idx

		return None

	def lookup(self, k):
		idx = self.lookup_pos(k)
		if idx is None:
			return self.last()
		return self.v[idx]
	
	def delete(self, n):
		if n >= len(self.v):
			return False

		del self.v[n]
		self.dirty = True

		return True

	def insert(self, ent):
		idx = self.lookup_pos(ent.k)
		if idx is None:
			self.v.append(ent)
		else:
			self.v.insert(idx, ent)
		self.dirty = True

