

import os
import struct
import zlib

from util import crcheader, writeobj, tryread


LOGR_ID_DATA = 'LOGR'
LOGR_ID_TXN_START = 'TXN '
LOGR_ID_TXN_COMMIT = 'TXNC'
LOGR_ID_TXN_ABORT = 'TXNA'
LOGR_DELETE = (1 << 0)


class MiscRecord(object):
	def __init__(self, name=None, v=0L):
		self.name = name
		self.v = v

	def deserialize(self, fd):
		data = tryread(fd, 8 + 4)
		if data is None:
			return False
		
		hdr = crcheader(data)
		if hdr is None:
			return False

		self.v = struct.unpack('<Q', hdr)[0]

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
		hdr = tryread(fd, 4 * 6)
		if hdr is None:
			return False
		(namsz, ksz, vsz,
		 self.recmask, self.txn_id) = struct.unpack('<IIIIQ', hdr)

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
	def __init__(self, dbdir, log_idx):
		self.dbdir = dbdir
		self.log_idx = log_idx
		self.fd = None
	
	def __del__(self):
		self.close()

	def open(self):
		try:
			name = "/log.%x" % (self.log_idx,)
			self.fd = os.open(self.dbdir + name,
					  os.O_CREAT | os.O_RDWR, 0666)
			st = os.fstat(self.fd)
			new_log = (st.st_size == 0)
			os.lseek(self.fd, 0, os.SEEK_END)
		except OSError:
			return False

		# initialize log file with header
		if new_log:
			if not trywrite(self.fd, 'LOGGER  '):
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

	def readreset(self):
		try:
			os.lseek(self.fd, 8, os.SEEK_SET)
		except OSError:
			return False

		return True
	
	def read(self):
		hdr = tryread(self.fd, 4)
		if hdr is None:
			return None

		if hdr == LOGR_ID_DATA:
			obj = DataRecord()

		elif (hdr == LOGR_ID_TXN_START or
		      hdr == LOGR_ID_TXN_COMMIT or
		      hdr == LOGR_ID_TXN_ABORT):
			obj = MiscRecord()

		else:
			return None

		if not obj.deserialize(self.fd):
			return None
		return obj


