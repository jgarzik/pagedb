
#
# Copyright 2012 Red Hat, Inc.
#
# Distributed under the MIT/X11 software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.
#

import struct
import zlib
import json
import re
import os
import os.path
import mmap
import uuid

from TableRoot import TableRoot
import Block
import PDcodec_pb2
from RecLogger import RecLogger, LOGR_DELETE, LOGR_ID_DATA, LOGR_ID_TABLE
from util import trywrite, isstr, readrecstr, writerecstr



class PDTableMeta(object):
	def __init__(self):
		# serialized
		self.name = ''
		self.uuid = uuid.uuid4()
		self.root_id = -1

		# only used at runtime
		self.root = None
		self.log_cache = {}
		self.log_del_cache = {}


class PDSuper(object):
	def __init__(self):
		self.version = 1
		self.uuid = uuid.uuid4()
		self.log_id = 1L
		self.next_txn_id = 1L
		self.next_file_id = 2L
		self.tables = {}
		self.dirty = False

	def deserialize(self, s):
		tup = readrecstr(s)
		if tup is None:
			return False
		recname = tup[0]
		data = tup[1]

		if recname != 'PGDB':
			return False

		obj = PDcodec_pb2.Superblock()
		try:
			obj.ParseFromString(data)
		except google.protobuf.message.DecodeError: 
			return None

		self.log_id = obj.log_id
		self.next_txn_id = obj.next_txn_id
		self.next_file_id = obj.next_file_id
		if (self.log_id < 1 or
		    self.next_txn_id < 1 or
		    self.next_file_id < 1):
			return False

		try:
			self.uuid = uuid.UUID(obj.uuid)
		except ValueError:
			return False

		for tm in obj.tables:
			tablemeta = PDTableMeta()
			tablemeta.name = tm.name
			tablemeta.root_id = tm.root_id

			try:
				tablemeta.uuid = uuid.UUID(tm.uuid)
			except ValueError:
				return False

			tables[tablemeta.name] = tablemeta

		return True

	def serialize(self):
		obj = PDcodec_pb2.Superblock()
		obj.uuid = self.uuid.hex
		obj.log_id = self.log_id
		obj.next_txn_id = self.next_txn_id
		obj.next_file_id = self.next_file_id

		for tablemeta in self.tables:
			tm = obj.tables.add()
			tm.name = tablemeta.name
			tm.uuid = tablemeta.uuid.hex
			tm.root_id = tablemeta.root_id

		r = writerecstr('PGDB', obj.SerializeToString())

		return r

	def new_fileid(self):
		rv = self.next_file_id
		self.next_file_id += 1
		self.dirty = True
		return rv


class PageTxn(object):
	def __init__(self, id):
		self.id = id
		self.log = []

	def get(self, k):
		for dr in reversed(self.log):
			if dr.key == k:
				if dr.recmask & LOGR_DELETE:
					return None
				return dr.v
		return None

	def exists(self, k):
		for dr in reversed(self.log):
			if dr.key == k:
				if dr.recmask & LOGR_DELETE:
					return False
				return True
		return False


class PageTable(object):
	def __init__(self, db, tablemeta):
		self.db = db
		self.tablemeta = tablemeta

	def put(self, txn, k, v):
		dr = self.db.logger.data(self.tablemeta, txn, k, v)
		if dr is None:
			return False

		txn.log.append(dr)

		return True

	def delete(self, txn, k):
		if not self.exists(txn, k):
			return False

		dr = self.db.logger.data(self.tablemeta, txn, k, None, True)
		if dr is None:
			return False

		txn.log.append(dr)

		return True

	def get(self, txn, k):

		if txn and txn.exists(k):
			return txn.get(k)
		if k in self.tablemeta.log_del_cache:
			return None
		if k in self.tablemeta.log_cache:
			return self.tablemeta.log_cache[k]

		ent = self.tablemeta.root.lookup(k)
		if ent is None:
			return None

		block = self.db.blockmgr.get(ent.file_id)
		if block is None:
			return None

		blkent = block.lookup(k)
		if blkent is None:
			return None

		return block.read_value(blkent)

	def exists(self, txn, k):
		if txn and txn.exists(k):
			return True
		if k in self.tablemeta.log_del_cache:
			return False
		if k in self.tablemeta.log_cache:
			return True

		ent = self.tablemeta.root.lookup(k)
		if ent is None:
			return False

		block = self.db.blockmgr.get(ent.file_id)
		if block is None:
			return False

		blkent = block.lookup(k)
		if blkent is None:
			return False

		return True


class PageDb(object):
	def __init__(self):
		self.dbdir = None
		self.super = None
		self.logger = None
		self.blockmgr = None

	def open(self, dbdir):
		self.dbdir = dbdir

		self.super = PDSuper()

		try:
			fd = os.open(dbdir + '/super', os.O_RDONLY)
			map = mmap.mmap(fd, 0, mmap.MAP_SHARED, mmap.PROT_READ)
			deser_ok = self.super.deserialize(map)
			map.close()
			os.close(fd)
			if not deser_ok:
				return False
		except OSError:
			return False

		if not self.read_logs():
			return False

		self.logger = RecLogger(dbdir, self.super.log_id)
		if not self.logger.open():
			return False

		self.blockmgr = Block.BlockManager(dbdir)

		return True

	def read_logdata(self, obj):
		try:
			tablemeta = self.super.tables[obj.table]
		except KeyError:
			return False

		if obj.recmask & LOGR_DELETE:
			tablemeta.log_del_cache[obj.key] = True
		else:
			tablemeta.log_cache[obj.key] = obj.value

		return True

	def read_logtable(self, obj):
		# FIXME: unsupported
		if obj.recmask & LOGR_DELETE:
			return False

		if obj.tabname in self.super.tables:
			return False

		tablemeta = PDTableMeta()
		tablemeta.name = obj.tabname
		tablemeta.root_id = obj.root_id
		tablemeta.root = TableRoot(self.dbdir, tablemeta.root_id)

		self.super.tables[obj.tabname] = tablemeta
		self.super.dirty = True

		return True

	def read_log(self, logger):
		while True:
			tup = logger.read()
			if tup is None:
				return True

			recname = tup[0]
			obj = tup[1]

			if recname == LOGR_ID_DATA:
				if not self.read_logdata(obj):
					return False

			elif recname == LOGR_ID_TABLE:
				if not self.read_logtable(obj):
					return False

	def read_logs(self):
		log_id = self.super.log_id
		while True:
			logger = RecLogger(self.dbdir, log_id)
			if not logger.open(True):
				if log_id == self.super.log_id:
					return False
				return True
			if not logger.readreset():
				return False
			if not self.read_log(logger):
				return False
			log_id += 1

	def create(self, dbdir):
		if not os.path.isdir(dbdir):
			return False

		self.dbdir = dbdir

		self.super = PDSuper()

		data = self.super.serialize()
		try:
			fd = os.open(dbdir + '/super',
				     os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0666)
			ok = trywrite(fd, data)
			os.fsync(fd)
			os.close(fd)
			if not ok:
				return False
		except OSError:
			return False

		self.logger = RecLogger(dbdir, self.super.log_id)
		if not self.logger.open():
			return False

		self.blockmgr = Block.BlockManager(dbdir)

		return True

	def open_table(self, name):
		try:
			tablemeta = self.super.tables[name]
		except KeyError:
			return None

		if tablemeta.root is None:
			root = TableRoot(self.dbdir, tablemeta.root_id)
			if not root.load():
				return None
			tablemeta.root = root

		return PageTable(self, tablemeta)

	def create_table(self, name):
		m = re.search('^\w+$', name)
		if m is None:
			return False

		if name in self.super.tables:
			return False

		tablemeta = PDTableMeta()
		tablemeta.name = name
		tablemeta.root_id = self.super.new_fileid()
		tablemeta.root = TableRoot(self.dbdir, tablemeta.root_id)
		if not tablemeta.root.dump():
			return False

		if not self.logger.tableop(tablemeta, None):
			return None

		self.super.tables[name] = tablemeta
		self.super.dirty = True

		return True

	def txn_begin(self):
		txn = PageTxn(self.super.next_txn_id)
		if not self.logger.txn_begin(txn):
			return None
		self.super.next_txn_id += 1
		self.super.dirty = True

		return txn

	def txn_commit(self, txn, sync=True):
		if not self.logger.txn_end(txn, True):
			return False
		if sync and not self.logger.sync():
			return False

		for dr in txn.log:
			if not self.read_logdata(dr):
				return False

		return True

	def txn_abort(self, txn):
		if not self.logger.txn_end(txn, False):
			return False
		return True

