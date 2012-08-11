
#
# Copyright 2012 Red Hat, Inc.
#
# Distributed under the MIT/X11 software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.
#

import struct
import zlib
import os

import PDcodec_pb2
from util import readrec, writepb


class TableRoot(object):
	def __init__(self, dbdir, root_id):
		self.dbdir = dbdir
		self.root_id = root_id
		self.v = []
		self.dirty = False

	def load(self):
		name = "/root.%x" % (self.root_id,)
		fd = os.open(self.dbdir + name, os.O_RDONLY)

		rc = self.deserialize(fd)

		os.close(fd)

		if not rc:
			return False

		self.dirty = False
		return True

	def dump(self):
		name = "/root.%x" % (self.root_id,)
		fd = os.open(self.dbdir + name,
			     os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0666)

		rc = self.serialize(fd)

		os.close(fd)

		if not rc:
			return False

		self.dirty = False
		return True

	def deserialize(self, fd):
		tup = readrec(fd)
		if tup is None:
			return False
		recname = tup[0]
		data = tup[1]
		if recname != 'ROOT':
			return False

		rootidx = PDcodec_pb2.RootIdx()
		try:
			rootidx.ParseFromString(data)
		except google.protobuf.message.DecodeError:
			return False

		self.v = []
		for rootent in rootidx.entries:
			self.v.append(rootent)

		return True

	def serialize(self, fd):
		rootidx = PDcodec_pb2.RootIdx()
		for ent in self.v:
			rootent = rootidx.entries.add()
			rootent.key = ent.key
			rootent.file_id = ent.file_id

		if not writepb(fd, 'ROOT', rootidx):
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
			if k <= self.v[idx].key:
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

