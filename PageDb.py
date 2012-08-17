
#
# Copyright 2012 Red Hat, Inc.
#
# Distributed under the MIT/X11 software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.
#

import struct
import json
import re
import os
import os.path
import mmap
import uuid

from TableRoot import TableRoot
import Block
import PDcodec_pb2
import RecLogger
from util import trywrite, isstr, readrecstr, writerecstr


class PDTableMeta(object):
	def __init__(self, super):
		# serialized
		self.name = ''
		self.uuid = uuid.uuid4()
		self.root_id = -1

		# only used at runtime
		self.super = super
		self.root = None
		self.log_cache = {}
		self.log_del_cache = set()

	def flush_rootidx(self):
		if not self.root.dirty:
			return True

		old_root_id = self.root.root_id

		self.root.root_id = self.super.new_fileid()

		if not self.root.dump():
			self.root.root_id = old_root_id
			return False

		self.root_id = self.root.root_id

		self.super.garbage_fileids.append(old_root_id)

		return True

	def checkpoint_initial(self):
		writer = Block.BlockWriter(self.super)

		keys = sorted(self.log_cache.keys())
		for key in keys:
			if not writer.push(key, self.log_cache[key]):
				return False
		if not writer.flush():
			return False

		self.root.v = writer.root_v
		self.root.dirty = True

		if not self.flush_rootidx():
			return False

		return True

	def checkpoint_block(self, blkent, add_recs, del_recs):
		# read old block data
		block = Block.Block(self.super.dbdir, blkent.file_id)
		if not block.open():
			return None
		blkvals = block.readall()
		if blkvals is None:
			return None

		# merge old block data (blkvals), new block data (add_recs),
		# and block data deletion notations (del_recs)
		# into a single sorted stream of key/value pairs
		writer = Block.BlockWriter(self.super)
		idx_old = 0
		idx_new = 0
		idx_del = 0
		while (idx_old < len(blkvals) and
		       idx_new < len(add_recs)):
			have_old = idx_old < len(blkvals)
			have_new = idx_new < len(add_recs)
			have_del = idx_del < len(del_recs)
			if (have_old and
			    ((not have_new) or
			     (blkvals[idx_old][0] <= add_recs[idx_new][0]))):
				tup = blkvals[idx_old]
				idx_old += 1
			else:
				tup = add_recs[idx_new]
				idx_new += 1

			if have_del and (tup[0] == del_recs[idx_del]):
				idx_del += 1
			else:
				if not writer.push(tup[0], tup[1]):
					return None

		if not writer.flush():
			return None

		return writer.root_v

	def checkpoint(self):
		if len(self.root.v) == 0:
			return self.checkpoint_initial()

		keys = sorted(self.log_cache.keys())
		del_keys = sorted(self.log_del_cache)
		keyidx = 0
		del_keyidx = 0
		blockidx = 0
		last_block = len(self.root.v) - 1

		new_root_v = []
		root_dirty = False

		while blockidx <= last_block:
			ent = self.root.v[blockidx]

			# accumulate new records belonging to this block
			add_recs = []
			while (keyidx < len(keys) and
			       ((keys[keyidx] <= ent.key) or
			        (blockidx == last_block))):
				tup = (keys[keyidx],
				       self.log_cache[keys[keyidx]])
				add_recs.append(tup)
				keyidx += 1

			# accumulate record deletions belonging to this block
			del_recs = []
			while (del_keyidx < len(del_keys) and
			       (del_keys[del_keyidx] <= ent.key)):
				del_recs.append(del_keys[del_keyidx])
				del_keyidx += 1

			# update block, or split into multiple blocks
			if len(add_recs) > 0 or len(del_recs) > 0:
				entlist = self.checkpoint_block(ent,
							add_recs, del_recs)
				if entlist is None:
					return False

				if (len(entlist) == 1 and
				    entlist[0].key == ent.key and
				    entlist[0].file_id == ent.file_id):
					new_root_v.append(ent)
				else:
					new_root_v.extend(entlist)
					root_dirty = True
			else:
				new_root_v.append(ent)

			blockidx += 1

		if root_dirty:
			self.root.v = new_root_v
			self.root.dirty = True
			if not self.flush_rootidx():
				return False

		return False

	def checkpoint_flush(self):
		self.log_cache = {}
		self.log_del_cache = set()


class PDSuper(object):
	def __init__(self, dbdir):
		self.version = 1
		self.uuid = uuid.uuid4()
		self.log_id = 1L
		self.next_txn_id = 1L
		self.next_file_id = 2L
		self.tables = {}
		self.dirty = False

		# only used at runtime
		self.dbdir = dbdir
		self.garbage_fileids = []

	def load(self):
		try:
			fd = os.open(self.dbdir + '/super', os.O_RDONLY)
			map = mmap.mmap(fd, 0, mmap.MAP_SHARED, mmap.PROT_READ)
			deser_ok = self.deserialize(map)
			map.close()
			os.close(fd)
			if not deser_ok:
				return False
		except OSError:
			return False

		return True

	def dump(self):
		data = self.serialize()
		try:
			fd = os.open(self.dbdir + '/super.tmp',
				     os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0666)
			ok = trywrite(fd, data)
			os.fsync(fd)
			os.close(fd)
			if not ok:
				os.unlink(self.dbdir + '/super.tmp')
				return False
		except OSError:
			return False

		try:
			os.rename(self.dbdir + '/super.tmp',
				  self.dbdir + '/super')
		except OSError:
			os.unlink(self.dbdir + '/super.tmp')
			return False

		self.dirty = False

		return True

	def deserialize(self, s):
		hdr = s[:8]
		if hdr != 'SUPER   ':
			return False

		tup = readrecstr(s[8:])
		if tup is None:
			return False
		recname = tup[0]
		data = tup[1]

		if recname != 'SUPR':
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
			tablemeta = PDTableMeta(self)
			tablemeta.name = tm.name
			tablemeta.root_id = tm.root_id

			try:
				tablemeta.uuid = uuid.UUID(tm.uuid)
			except ValueError:
				return False

			self.tables[tablemeta.name] = tablemeta

		return True

	def serialize(self):
		obj = PDcodec_pb2.Superblock()
		obj.uuid = self.uuid.hex
		obj.log_id = self.log_id
		obj.next_txn_id = self.next_txn_id
		obj.next_file_id = self.next_file_id

		for tablemeta in self.tables.itervalues():
			tm = obj.tables.add()
			tm.name = unicode(tablemeta.name)
			tm.uuid = tablemeta.uuid.hex
			tm.root_id = tablemeta.root_id

		r = 'SUPER   '
		r += writerecstr('SUPR', obj.SerializeToString())

		return r

	def new_fileid(self):
		rv = self.next_file_id
		self.next_file_id += 1
		self.dirty = True
		return rv

	def new_txnid(self):
		rv = self.next_txn_id
		self.next_txn_id += 1
		self.dirty = True
		return rv


class PageTxn(object):
	def __init__(self, id):
		self.id = id
		self.log = []

	def get(self, k):
		for dr in reversed(self.log):
			if dr.key == k:
				if dr.recmask & RecLogger.LOGR_DELETE:
					return None
				return dr.v
		return None

	def exists(self, k):
		for dr in reversed(self.log):
			if dr.key == k:
				if dr.recmask & RecLogger.LOGR_DELETE:
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

		self.super = PDSuper(dbdir)
		if not self.super.load():
			return False

		if not self.read_logs():
			return False

		self.logger = RecLogger.RecLogger(dbdir, self.super.log_id)
		if not self.logger.open():
			return False

		self.blockmgr = Block.BlockManager(dbdir)

		return True

	def apply_logdata(self, obj):
		try:
			tablemeta = self.super.tables[obj.table]
		except KeyError:
			return False

		if obj.recmask & RecLogger.LOGR_DELETE:
			tablemeta.log_del_cache.add(obj.key)
			try:
				del tablemeta.log_cache[obj.key]
			except KeyError:
				pass
		else:
			tablemeta.log_cache[obj.key] = obj.value
			tablemeta.log_del_cache.discard(obj.key)

		return True

	def read_logtable(self, obj):
		# TODO: logged table deletion unsupported
		if obj.recmask & RecLogger.LOGR_DELETE:
			return False

		if obj.tabname in self.super.tables:
			return False

		tablemeta = PDTableMeta(self.super)
		tablemeta.name = obj.tabname
		tablemeta.root_id = obj.root_id
		tablemeta.root = TableRoot(self.dbdir, tablemeta.root_id)

		self.super.tables[obj.tabname] = tablemeta
		self.super.dirty = True

		return True

	def read_superop(self, obj):
		if obj.op == PDcodec_pb2.LogSuperOp.INC_TXN:
			self.super.next_txn_id += 1
		elif obj.op == PDcodec_pb2.LogSuperOp.INC_FILE:
			self.super.next_file_id += 1
		else:
			return False
		self.super.dirty = True
		return True

	def read_logtxn_start(self, txns, obj):
		if obj.txn_id in txns:
			return False

		txn = PageTxn(obj.txn_id)
		txns[obj.txn_id] = txn

		return True

	def read_logtxn_abort(self, txns, obj):
		if obj.txn_id not in txns:
			return False

		del txns[obj.txn_id]

		return True

	def read_logtxn_commit(self, txns, obj):
		if obj.txn_id not in txns:
			return False

		txn = txns[obj.txn_id]
		del txns[obj.txn_id]

		for dr in txn.log:
			if not self.apply_logdata(dr):
				return False

		return True

	def read_logdata(self, txns, obj):
		if obj.txn_id not in txns:
			return False

		txn = txns[obj.txn_id]
		txn.log.append(obj)

		return True

	def read_log(self, logger):
		txns = {}
		while True:
			tup = logger.read()
			if tup is None:
				return True

			recname = tup[0]
			obj = tup[1]

			if recname == RecLogger.LOGR_ID_TXN_START:
				if not self.read_logtxn_start(txns, obj):
					return False

			elif recname == RecLogger.LOGR_ID_TXN_COMMIT:
				if not self.read_logtxn_commit(txns, obj):
					return False

			elif recname == RecLogger.LOGR_ID_TXN_ABORT:
				if not self.read_logtxn_abort(txns, obj):
					return False

			elif recname == RecLogger.LOGR_ID_DATA:
				if not self.read_logdata(txns, obj):
					return False

			elif recname == RecLogger.LOGR_ID_TABLE:
				if not self.read_logtable(obj):
					return False

			elif recname == RecLogger.LOGR_ID_SUPER:
				if not self.read_superop(obj):
					return False

	def read_logs(self):
		log_id = self.super.log_id
		while True:
			logger = RecLogger.RecLogger(self.dbdir, log_id)
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

		self.super = PDSuper(dbdir)
		if not self.super.dump():
			return False

		self.logger = RecLogger.RecLogger(dbdir, self.super.log_id)
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

		tablemeta = PDTableMeta(self.super)
		tablemeta.name = name
		tablemeta.root_id = self.super.new_fileid()
		tablemeta.root = TableRoot(self.dbdir, tablemeta.root_id)
		if not tablemeta.root.dump():
			return False

		if not self.logger.superop(self.super,
					   PDcodec_pb2.LogSuperOp.INC_FILE):
			return None
		if not self.logger.tableop(tablemeta, None):
			return None

		self.super.tables[name] = tablemeta
		self.super.dirty = True

		return True

	def txn_begin(self):
		if not self.logger.superop(self.super,
					   PDcodec_pb2.LogSuperOp.INC_TXN):
			return None
		txn = PageTxn(self.super.new_txnid())
		if not self.logger.txn_begin(txn):
			return None

		return txn

	def txn_commit(self, txn, sync=True):
		if not self.logger.txn_end(txn, True):
			return False
		if sync and not self.logger.sync():
			return False

		for dr in txn.log:
			if not self.apply_logdata(dr):
				return False

		return True

	def txn_abort(self, txn):
		if not self.logger.txn_end(txn, False):
			return False
		return True

	def checkpoint(self):
		for tablemeta in self.super.tables.itervalues():
			if not tablemeta.checkpoint():
				return False

		# alloc new log id, open new log
		new_log_id = self.super.new_fileid()
		new_logger = RecLogger.RecLogger(self.dbdir, new_log_id)
		if not new_logger.open():
			self.super.garbage_fileids.append(new_log_id)
			return False

		# swap in new log id into superblock, write superblock
		old_log_id = self.super.log_id
		self.super.log_id = new_log_id
		if not self.super.dump():
			self.super.log_id = old_log_id
			self.super.garbage_fileids.append(new_log_id)
			return False

		# if we succeeded in switching to the newly committed
		# data, flush cached log data just written to storage
		for tablemeta in self.super.tables.itervalues():
			tablemeta.checkpoint_flush()

		# overwrite old logger, closing old log file
		self.super.garbage_fileids.append(old_log_id)
		self.logger = new_logger

		# TODO: delete super.garbage_fileids

		return True

