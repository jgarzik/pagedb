
import struct
import zlib
import json
import re
import os
import os.path
import uuid

import TableRoot
import RecLogger
import Block
from util import tryread



class PDTableMeta(object):
	def __init__(self):
		self.name = ''
		self.uuid = uuid.uuid4()
		self.root_id = -1
		self.root = None

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
		self.log_idx = 1L
		self.next_file_id = 1L
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
		    'log_idx' not in jv or
		    not isstr(jv['log_idx']) or
		    re.search('^[\dA-Fa-f]+$', jv['log_idx']) is None or
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

		self.log_idx = long(jv['log_idx'], 16)
		if self.log_idx < 1:
			return False

		self.next_file_id = long(jv['next_file_id'], 16)
		if self.next_file_id < 1:
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
		jv['log_idx'] = hex(self.log_idx)
		jv['next_file_id'] = hex(self.next_file_id)

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


class PageTxn(object):
	def __init__(self, id):
		self.id = id
		self.log_cache = {}
		self.log_del_cache = {}


class PageTable(object):
	def __init__(self, db, tablemeta):
		self.db = db
		self.tablemeta = tablemeta

	def put(self, txn, k, v):
		self.db.put(self.tablemeta, txn, k, v)
	
	def get(self, txn, k):
		return self.db.get(self.tablemeta, txn, k)
		
	def delete(self, txn, k):
		return self.db.delete(self.tablemeta, txn, k)
		
	def exists(self, txn, k):
		return self.db.exists(self.tablemeta, txn, k)
		

class PageDb(object):
	def __init__(self):
		self.dbdir = None
		self.readonly = False
		self.super = None
		self.log_cache = {}
		self.log_del_cache = {}
		self.logger = None
		self.blockmgr = None
	
	def open(self, dbdir, readonly=False):
		self.dbdir = dbdir
		self.readonly = readonly

		try:
			fd = os.open(dbdir + '/super', os.O_RDONLY)
			fdata = os.read(fd, 16 * 1024 * 1024)
			os.close(fd)
		except OSError:
			return False

		self.super = PDSuper()
		if not self.super.deserialize(fdata):
			return False

		self.logger = RecLogger.RecLogger(dbdir, self.super.log_idx)
		if not self.logger.open():
			return False

		self.blockmgr = Block.BlockManager(dbdir)

		return True

	def create(self, dbdir):
		if not os.path.isdir(dbdir):
			return False

		self.super = PDSuper()
		self.super.dirty = True

		self.logger = RecLogger.RecLogger(dbdir)
		if not self.logger.open():
			return False

		self.blockmgr = Block.BlockManager(dbdir)

		return True

	def open_table(self, name):
		try:
			tablemeta = self.super.tables[name]
		except KeyError:
			return None

		if tablemeta.root_id < 0:
			tablemeta.root_id = self.super.next_file_id
			self.super.next_file_id += 1
			self.super.dirty = True

		if tablemeta.root is None:
			root = TableRoot.TableRoot(self.dbdir, tablemeta.root_id)
			if not root.open():
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
		
		self.super.tables[name] = tablemeta
		self.super.dirty = True

		return True

	def txn_begin(self):
		txn = PageTxn(self.super.log_idx)
		if not self.logger.txn_begin(txn):
			return None
		self.super.log_idx += 1

		return txn
	
	def txn_commit(self, txn):
		if (not self.logger.txn_end(txn, True) or
		    not self.logger.sync()):
			return False

		for k, v in txn.log_cache.iteritems():
			self.log_cache[k] = v
		for k in txn.log_del_cache.iterkeys():
			self.log_del_cache[k] = True

		return True

	def txn_abort(self, txn):
		if not self.logger.txn_end(txn, False):
			return False
		return True

	def put(self, tablemeta, txn, k, v):
		if not self.logger.data(tablemeta, txn, k, v):
			return False

		try:
			del txn.log_del_cache[k]
		except KeyError:
			pass

		txn.log_cache[k] = v

		return True

	def delete(self, tablemeta, txn, k):
		if not self.exists(tablemeta, txn, k):
			return False
		if not self.logger.data(tablemeta, txn, k, None, True):
			return False

		try:
			del txn.log_cache[k]
		except KeyError:
			pass

		txn.log_del_cache[k] = True
		return True

	def get(self, tablemeta, txn, k):

		if k in txn.log_del_cache:
			return None
		if k in self.log_del_cache:
			return None

		if k in txn.log_cache:
			return txn.log_cache[k]
		if k in self.log_cache:
			return self.log_cache[k]

		ent = tablemeta.root.lookup(k)
		if ent is None:
			return None

		block = self.blockmgr.get(ent.file_id)
		if block is None:
			return None

		blkent = block.lookup(k)
		if blkent is None:
			return None

		return block.read_value(blkent)

	def exists(self, tablemeta, txn, k):
		if k in txn.log_del_cache:
			return False
		if k in self.log_del_cache:
			return False

		if k in txn.log_cache:
			return True
		if k in self.log_cache:
			return True

		ent = tablemeta.root.lookup(k)
		if ent is None:
			return False

		block = self.blockmgr.get(ent.file_id)
		if block is None:
			return False

		blkent = block.lookup(k)
		if blkent is None:
			return False

		return True

