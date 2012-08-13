
#
# Copyright 2012 Red Hat, Inc.
#
# Distributed under the MIT/X11 software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.
#

import struct
import os
import mmap

import PDcodec_pb2
from util import trywrite, updcrc, readrecstr, writerecstr


MIN_BLK_SZ = 1024
TARGET_MIN_BLK_SZ = 2 * 1024 * 1024
TARGET_MAX_BLK_SZ = 8 * 1024 * 1024
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
		self.v = ''
		self.v_len = None
		self.k_len = None

	def deserialize_hdr(self, s):
		(self.k_len, self.v_len) = struct.unpack('<II', s)

	def serialize(self):
		r = struct.pack('<II', len(self.k), len(self.v))
		r += self.k
		r += self.v
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
		self.close()

	def close(self):
		if self.map is not None:
			try:
				self.map.close()
			except OSError:
				pass
			self.map = None

		if self.fd is not None:
			try:
				os.close(self.fd)
			except OSError:
				pass
			self.fd = None

	def open(self):
		# open and mmap file
		try:
			name = "/block.%x" % (self.file_id,)
			self.fd = os.open(self.dbdir + name, os.O_RDONLY)
			self.st = os.fstat(self.fd)
			if (self.st.st_size > MAX_BLK_SZ):
				return False
			self.map = mmap.mmap(self.fd, 0, mmap.MAP_SHARED,
					     mmap.PROT_READ)
		except OSError:
			return False

		# verify magic number header
		if self.map[:8] != 'BLOCK   ':
			return False

		# unpack and validate trailer
		tup = readrecstr(self.map[-((4 * 5) + 4):-4])
		if tup is None:
			return False
		if tup[0] != 'DTRL':
			return False
		self.arrpos, self.n_keys = struct.unpack('<II', tup[1])

		if self.st.st_size < (self.arrpos + (self.n_keys * 8)):
			return False

		return True

	def create(self):
		try:
			name = "/block.%x" % (self.file_id,)
			self.fd = os.open(self.dbdir + name,
					  os.O_CREAT | os.O_EXCL | os.O_WRONLY)
		except OSError:
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
			keypos = blkidx.entpos + (4 * 2)
			test_key = self.map[keypos : keypos + blkidx.k_len]
			if k == test_key:
				return self.read_entkey(blkidx, k)
			if k < test_key:
				return None

		return None

	def read_entkey(self, blkidx, k=None):
		blkent = BlockEnt()
		blkent.deserialize_hdr(self.map[blkidx.entpos :
						blkidx.entpos + (4 * 2)])
		if k is None:
			kpos = blkidx.entpos + (4 * 2)
			blkent.k = self.map[kpos : kpos + self.k_len]
		else:
			blkent.k = k
		return blkent

	def read_value(self, blkidx, blkent):
		spos = blkidx.entpos + (4 * 2) + blkent.k_len
		epos = spos + blkent.v_len
		if epos > self.st.st_size:
			return None

		return self.map[spos:epos]

	def readall(self):
		ret_data = []
		for idx in xrange(self.n_keys):
			blkidx = self.getblkidx(idx)
			blkent = self.read_entkey(blkidx)
			value = self.read_value(blkidx, blkent)

			tup = (blkent.k, value)

			ret_data.append(tup)

		return ret_data

	def write_values(self, vals):
		idxs = []

		# section 1: header
		hdr = 'BLOCK   '
		if not trywrite(self.fd, hdr):
			return None
		pos = len(hdr)
		crc = updcrc(hdr, 0)

		# section 2: write key/value pairs in sorted order
		for tup in vals:
			key = tup[0]
			val = tup[1]

			blkent = BlockEnt()
			blkent.k = key
			blkent.v = val

			blkidx = BlockIdx()
			blkidx.entpos = pos
			blkidx.k_len = len(blkent.k)

			rec_data = writerecstr('DATA', blkent.serialize())

			if not trywrite(self.fd, rec_data):
				return None

			pos += len(rec_data)
			crc = updcrc(rec_data, crc)

			idxs.append(blkidx)

		arrpos = pos + 8

		# section 3: write fixed-length key index in sorted order
		arrdata = []
		for idx in idxs:
			arrdata.append(idx.serialize())

		rec_data = writerecstr('DIDX', ''.join(arrdata))
		if not trywrite(self.fd, rec_data):
			return None

		crc = updcrc(rec_data, crc)

		# section 4: data trailer
		raw_data = struct.pack('<II', arrpos, len(vals))
		rec_data = writerecstr('DTRL', raw_data)
		if not trywrite(self.fd, rec_data):
			return None

		crc = updcrc(rec_data, crc)

		# section 5: whole-file CRC trailer
		data = struct.pack('<I', crc)
		if not trywrite(self.fd, data):
			return None

		return vals[-1][0]


class BlockWriter(object):
	def __init__(self, super):
		self.super = super
		self.block = None
		self.recs = []
		self.rec_bytes = 0
		self.root_v = []

	def flush(self):
		if self.block is None:
			return True
		last_key = self.block.write_values(self.recs)
		if last_key is None:
			return False
		self.block.close()

		rootent = PDcodec_pb2.RootEnt()
		rootent.key = last_key
		rootent.file_id = self.block.file_id

		self.root_v.append(rootent)

		self.block = None
		self.recs = []
		self.rec_bytes = 0

		return True

	def push(self, key, value):
		if self.block is None:
			self.block = Block(self.super.dbdir,
					   self.super.new_fileid())
			if not self.block.create():
				return False

		tup = (key, value)
		self.recs.append(tup)
		self.rec_bytes += len(key) + len(value)

		if self.rec_bytes > TARGET_MIN_BLK_SZ:
			return self.flush()
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

