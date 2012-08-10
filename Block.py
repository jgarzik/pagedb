
#
# Copyright 2012 Red Hat, Inc.
#
# Distributed under the MIT/X11 software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.
#

import struct
import os
import mmap


MIN_BLK_SZ = 1024
MAX_BLK_SZ = 16 * 1024 * 1024


class BlockIdx(object):
	def __init__(self):
		self.entpos = -1
		self.k_len = 0

	def deserialize(self, s):
		(self.entpos, self.k_len) = struct.unpack('<II', s)
		return True

	def serialize(self):
		r = struct.pack('<II', self.entpos, self.k_len)
		return r


class BlockEnt(object):
	def __init__(self):
		self.k = ''
		self.v_pos = -1
		self.v_len = 0

	def deserialize_hdr(self, s):
		(k_len, self.v_pos, self.v_len) = struct.unpack('<III', s)

	def serialize(self):
		r = struct.pack('<III', len(self.k), self.v_pos, self.v_len)
		r += self.k
		return r


class Block(object):
	def __init__(self, dbdir, file_id):
		self.dbdir = dbdir
		self.fd = None
		self.st = None
		self.map = None
		self.file_id = file_id
		self.n_keys = 0
		self.arrpos = -1

	def __del__(self):
		if self.map is not None:
			try:
				self.map.close()
			except OSError:
				pass

		if self.fd is not None:
			try:
				os.close(self.fd)
			except OSError:
				pass

	def open(self):
		# open and mmap file
		try:
			name = "%x" % (self.file_id,)
			self.fd = os.open(dbdir + '/' + name, os.O_RDONLY)
			self.st = os.fstat(self.fd)
			if (self.st.st_size < MIN_BLK_SZ or
			    self.st.st_size > MAX_BLK_SZ):
				return False
			self.map = mmap.mmap(self.fd, 0, mmap.MAP_SHARED,
					     mmap.PROT_READ)
		except OSError:
			return False

		# verify magic number
		if self.map[:8] != 'BLOCK   ':
			return False

		# unpack and validate trailer
		trailer = self.map[-12:-4]
		self.arrpos, self.n_keys = struct.unpack('<II', trailer)

		if self.st.st_size < (self.arrpos + (self.n_keys * 8)):
			return False

		return True

	def getblkidx(self, idx):
		# validate idx
		pos = self.arrpos + (idx * 8)
		if pos + 8 > self.st.st_size:
			return None

		# unpack index entry
		blkidx = BlockIdx()
		blkidx.deserialize(self.map[pos:pos+8])

		# validate index entry
		if blkidx.entpos + (4 * 3) + blkidx.k_len > self.st.st_size:
			return None

		return blkidx

	def lookup(self, k):
		# TODO: bisect
		# in-order linear search, key >= ours is found
		for idx in xrange(self.n_keys):
			# read position array for key offset into
			blkidx = self.getblkidx(idx)
			if blkidx is None:
				return None

			# test key against search key
			keypos = blkidx.entpos + (4 * 3)
			test_key = self.map[keypos : keypos + blkidx.k_len]
			if k == test_key:
				return self.read_ent(blkidx, k)
			if k < test_key:
				return None

		return None

	def read_ent(self, blkidx, k):
		blkent = BlockEnt()
		blkent.k = k
		blkent.deserialize_hdr(self.map[blkidx.entpos :
						blkidx.entpos + (4 * 3)])
		return blkent

	def read_value(self, blkent):
		spos = blkent.v_pos
		epos = spos + blkent.v_len
		if epos > self.st.st_size:
			return None

		return self.map[spos:epos]

	def write_values(self, d):
		keys = sorted(d.keys())
		ents = []
		idxs = []

		# section 1: header
		magic = 'BLOCK   '
		if not trywrite(self.fd, magic):
			return False
		pos = len(magic)
		crc = updcrc(magic, 0)

		# section 2: write values in sorted order
		for key in keys:
			blkent = BlockEnt()
			blkent.k = key
			blkent.v_pos = pos
			blkent.v_len = len(d[key])

			if not trywrite(self.fd, d[key]):
				return False

			pos += blkent.v_len
			crc = updcrc(d[key], crc)

			ents.append(blkent)

		# section 3: write keys in sorted order
		for ent in ents:
			blkidx = BlockIdx()
			blkidx.entpos = pos
			blkidx.k_len = len(ent.k)

			data = ent.serialize()

			if not trywrite(self.fd, data):
				return False

			pos += len(data)
			crc = updcrc(data, crc)

			idxs.append(blkidx)

		arrpos = pos

		# section 4: write fixed-length key index in sorted order
		for idx in idxs:
			data = idx.serialize()

			if not trywrite(self.fd, data):
				return False

			crc = updcrc(data, crc)

		# section 5: data trailer
		data = struct.pack('<II', arrpos, len(keys))
		if not trywrite(self.fd, data):
			return False

		crc = updcrc(data, crc)

		# section 6: CRC trailer
		data = struct.pack('<I', crc)
		if not trywrite(self.fd, data):
			return False

		return True


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

