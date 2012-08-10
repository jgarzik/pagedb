
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
from RecLogger import RecLogger, LOGR_DELETE, LOGR_ID_DATA, LOGR_ID_TABLE
from util import trywrite, crcheader, isstr



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

	def deserialize(self, table_k, table_v):
		if (not isstr(table_k) or
		    not isinstance(table_v, dict) or
		    'root_id' not in table_v or
		    not isstr(table_v['root_id']) or
		    re.search('^[\dA-Fa-f]+$', table_v['root_id']) is None or
		    'uuid' not in table_v or
		    not isstr(table_v['uuid'])):
			return False

		m = re.search('^\w+$', table_k)
		if m is None:
			return False

		self.root_id = long(table_v['root_id'], 16)

		self.name = table_k
		try:
			self.uuid = uuid.UUID(table_v['uuid'])
		except ValueError:
			return False

		return True

	def serialize(self):
		rv = {
			'uuid' : self.uuid.hex,
			'root_id' : hex(self.root_id),
		}
		return (self.name, rv)


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
		data_str = crcheader(s)
		if data_str is None:
			return False
		if data_str[:6] != 'PAGEDB':
			return False

		try:
			jv = json.loads(data_str[6:])
		except ValueError:
			return False

		if (not isinstance(jv, dict) or
		    'uuid' not in jv or
		    not isstr(jv['uuid']) or
		    'log_id' not in jv or
		    not isstr(jv['log_id']) or
		    re.search('^[\dA-Fa-f]+$', jv['log_id']) is None or
		    'next_txn_id' not in jv or
		    not isstr(jv['next_txn_id']) or
		    re.search('^[\dA-Fa-f]+$', jv['next_txn_id']) is None or
		    'next_file_id' not in jv or
		    not isstr(jv['next_file_id']) or
		    re.search('^[\dA-Fa-f]+$', jv['next_file_id']) is None or
		    'version' not in jv or
		    not isinstance(jv['version'], int) or
		    'tables' not in jv or
		    not isinstance(jv['tables'], dict)):
			return False

		self.version = jv['version']
		if self.version > 1:
			return False

		self.log_id = long(jv['log_id'], 16)
		self.next_txn_id = long(jv['next_txn_id'], 16)
		self.next_file_id = long(jv['next_file_id'], 16)
		if (self.log_id < 1 or
		    self.next_txn_id < 1 or
		    self.next_file_id < 1):
			return False

		try:
			self.uuid = uuid.UUID(jv['uuid'])
		except ValueError:
			return False

		for table_k, table_v in jv['tables'].iteritems():
			tablemeta = PDTableMeta()
			if not tablemeta.deserialize(table_k, table_v):
				return False

			tables[tablemeta.name] = tablemeta

		return True

	def serialize(self):
		jv = {}
		jv['version'] = self.version
		jv['uuid'] = self.uuid.hex
		jv['log_id'] = "%x" % (self.log_id,)
		jv['next_txn_id'] = "%x" % (self.next_txn_id,)
		jv['next_file_id'] = "%x" % (self.next_file_id,)

		jtables = {}

		for tablemeta in self.tables:
			(pd_k, pd_v) = tablemeta.serialize()
			jtables[pd_k] = pd_v

		jv['tables'] = jtables

		# magic header, json data
		r = 'PAGEDB'
		r += json.dumps(jv)

		# checksum footer
		crc = zlib.crc32(r) & 0xffffffff
		r += struct.pack('<I', crc)

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
			if dr.k == k:
				if dr.recmask & LOGR_DELETE:
					return None
				return dr.v
		return None

	def exists(self, k):
		for dr in reversed(self.log):
			if dr.k == k:
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
			tablemeta.log_del_cache[obj.k] = True
		else:
			tablemeta.log_cache[obj.k] = obj.v

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
			obj = logger.read()
			if obj is None:
				return True

			if obj.name == LOGR_ID_DATA:
				if not self.read_logdata(obj):
					return False

			elif obj.name == LOGR_ID_TABLE:
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

