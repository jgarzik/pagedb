
import struct
import zlib
import json
import re
import os
import uuid


LOGR_ID_DATA = 'LOGR'
LOGR_ID_TXN_START = 'TXN '
LOGR_ID_TXN_COMMIT = 'TXNC'
LOGR_ID_TXN_ABORT = 'TXNA'
LOGR_DELETE = (1 << 0)


def isstr(s):
	if isinstance(s, str) or isinstance(s, unicode):
		return True
	return False

def crcheader(s):
	if len(s) < 4:
		return None
	hdr = s[:-4]

	crc_str = s[-4:]
	crc_in = struct.unpack('<I', crc_str)[0]

	crc = zlib.crc32(hdr) & 0xffffffff
	if crc != crc_in:
		return None
	
	return hdr

def tryread(fd, n):
	try:
		data = os.read(fd, n)
	except OSError:
		return None
	if len(data) != n:
		return None
	return data
		
def trywrite(fd, data):
	try:
		bytes = os.write(fd, data)
	except OSError:
		return False
	if bytes != len(data):
		return False
	return True

def writeobj(fd, obj):
	data = obj.serialize()
	return trywrite(fd, data)


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


class MiscRecord(object):
	def __init__(self, name=None, v=0L):
		self.name = name
		self.v = v

	def deserialize(self, fd):
		try:
			data = os.read(fd, 4 * 4)
		except:
			return False
		if len(data) != (4 * 4):
			return False
		
		hdr = crcheader(data)
		if hdr is None:
			return False

		self.name = hdr[:4]
		self.v = struct.unpack('<Q', hdr[4:])[0]

		return True
	
	def serialize(self):
		r = self.name
		r += struct.pack('<Q', self.v)

		crc = zlib.crc32(r) & 0xffffffff
		r += struct.pack('<I', crc)

		return r


class DataRecord(object):
	def __init__(self):
		self.name = LOGR_ID_DATA
		self.table = None
		self.txn_id = -1L
		self.k = ''
		self.v = ''
		self.recmask = 0

	def deserialize(self, fd):
		try:
			hdr = os.read(fd, 4 * 7)
		except:
			return False
		if len(hdr) != (4 * 7):
			return False
		if hdr[:4] != LOGR_ID_DATA:
			return False
		(namsz, ksz, vsz,
		 self.recmask, self.txn_id) = struct.unpack('<IIIIQ', hdr[4:])

		try:
			self.table = os.read(fd, namsz)
			self.k = os.read(fd, ksz)
			self.v = os.read(fd, vsz)
			crcstr = os.read(fd, 4)
		except:
			return False

		crc_in = struct.unpack('<I', crcstr)[0]

		recdata = hdr + self.table + self.k + self.v
		crc = zlib.crc32(recdata) & 0xffffffff
		if crc != crc_in:
			return False

		return True
	
	def serialize(self):
		r = LOGR_ID_DATA
		r += struct.pack('<IIIIQ', len(self.table), len(self.k),
				 len(self.v), self.recmask, self.txn_id)
		r += self.table
		r += self.k
		r += self.v

		# checksum footer
		crc = zlib.crc32(r) & 0xffffffff
		r += struct.pack('<I', crc)

		return r


class RecLogger(object):
	def __init__(self, dbdir):
		self.dbdir = dbdir
		self.fd = None
	
	def __del__(self):
		self.close()

	def open(self):
		try:
			self.fd = os.open(self.dbdir + '/log',
					  os.O_CREAT | os.O_WRONLY |
					  os.O_APPEND, 0666)
		except OSError:
			return False

		return True

	def close(self):
		if self.fd is None:
			return
		try:
			os.close(self.fd)
		except:
			pass
		self.fd = None
	
	def sync(self):
		try:
			os.fsync(self.fd)
		except OSError:
			return False
		return True

	def data(self, tablemeta, txn, k, v, delete=False):
		dr = DataRecord()
		dr.txn_id = txn.id
		dr.table = tablemeta.name
		dr.k = k
		if delete:
			dr.recmask |= LOGR_DELETE
			dr.v = ''
		else:
			dr.v = v

		return writeobj(self.fd, dr)

	def txn_begin(self, txn):
		mr = MiscRecord(LOGR_ID_TXN_START, txn.id)
		return writeobj(self.fd, mr)

	def txn_end(self, txn, commit):
		if commit:
			mr = MiscRecord(LOGR_ID_TXN_COMMIT, txn.id)
		else:
			mr = MiscRecord(LOGR_ID_TXN_ABORT, txn.id)
		return writeobj(self.fd, mr)


class PageTxn(object):
	def __init__(self, id):
		self.id = id
		self.log_cache = {}
		self.log_del_cache = {}


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
			self.dirty = True
			return True

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

		self.logger = RecLogger(dbdir)
		if not self.logger.open():
			return False

		self.blockmgr = BlockManager(dbdir)

		return True

	def table(self, name):
		try:
			tablemeta = self.super.tables[name]
		except KeyError:
			return None

		if tablemeta.root_id < 0:
			tablemeta.root_id = self.super.next_file_id
			self.super.next_file_id += 1
			self.super.dirty = True

		if tablemeta.root is None:
			root = TableRoot(self.dbdir, tablemeta.root_id)
			if not root.open():
				return None
			tablemeta.root = root

		return PageTable(self, tablemeta)

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

