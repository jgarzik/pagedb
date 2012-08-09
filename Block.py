

import os


class BlockEnt(object):
	def __init__(self):
		self.k = ''
		self.pos = -1
	

class Block(object):
	def __init__(self, dbdir, file_id):
		self.dbdir = dbdir
		self.fd = None
		self.file_id = file_id

	def __del__(self):
		if self.fd is None:
			return
		try:
			os.close(self.fd)
		except OSError:
			pass

	def open(self):
		try:
			name = "%x" % (self.file_id,)
			self.fd = os.open(dbdir + '/' + name, os.O_RDONLY)
		except OSError:
			return False

		return True

	def lookup(self, k):
		#FIXME
		pass
	
	def read_value(self, blkent):
		#FIXME
		pass


class BlockManager(object):
	def __init__(self, dbdir):
		self.dbdir = dbdir
		self.cache = {}

	def get(self, file_id):
		if file_id in self.cache:
			return self.cache[file_id]

		block = Block(self.dbdir, file_id)
		if not block.open():
			return None

		self.cache[file_id] = block

		return block

