
#
# Copyright 2012 Red Hat, Inc.
#
# Distributed under the MIT/X11 software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.
#

import os
import struct
import zlib

from util import crcheader, writeobj, tryread, trywrite


LOGR_ID_DATA = 'LOGR'
LOGR_ID_TXN_START = 'TXN '
LOGR_ID_TXN_COMMIT = 'TXNC'
LOGR_ID_TXN_ABORT = 'TXNA'
LOGR_ID_TABLE = 'LTBL'
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
		r = struct.pack('<Q', self.v)

		crc = zlib.crc32(r) & 0xffffffff
		r += struct.pack('<I', crc)

		return self.name + r


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

		self.table = os.read(fd, namsz)
		self.k = os.read(fd, ksz)
		self.v = os.read(fd, vsz)
		crcstr = os.read(fd, 4)

		crc_in = struct.unpack('<I', crcstr)[0]

		recdata = LOGR_ID_DATA + hdr + self.table + self.k + self.v
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


class TableRecord(object):
	def __init__(self):
		self.name = LOGR_ID_TABLE
		self.tabname = ''
		self.recmask = 0
		self.root_id = 0L
		self.txn_id = 0L

	def deserialize(self, fd):
		hdr = tryread(fd, 4+4+8+8)
		if hdr is None:
			return False
		(namsz, self.recmask,
		 self.root_id, self.txn_id) = struct.unpack('<IIQQ', hdr)

		self.tabname = os.read(fd, namsz)
		crcstr = os.read(fd, 4)

		crc_in = struct.unpack('<I', crcstr)[0]

		recdata = LOGR_ID_TABLE + hdr + self.tabname
		crc = zlib.crc32(recdata) & 0xffffffff
		if crc != crc_in:
			return False

		return True

	def serialize(self):
		r = LOGR_ID_TABLE
		r += struct.pack('<IIQQ', len(self.tabname),
				 self.recmask, self.root_id, self.txn_id)
		r += self.tabname

		# checksum footer
		crc = zlib.crc32(r) & 0xffffffff
		r += struct.pack('<I', crc)

		return r


class RecLogger(object):
	def __init__(self, dbdir, log_id):
		self.dbdir = dbdir
		self.log_id = log_id
		self.fd = None
		self.readonly = False

	def __del__(self):
		self.close()

	def open(self, readonly=False):
		try:
			name = "/log.%x" % (self.log_id,)
			if readonly:
				flags = os.O_RDONLY
			else:
				flags = os.O_CREAT | os.O_RDWR
			self.fd = os.open(self.dbdir + name, flags, 0666)
			st = os.fstat(self.fd)
			new_log = (st.st_size == 0)
			os.lseek(self.fd, 0, os.SEEK_END)
		except OSError:
			return False

		self.readonly = readonly

		# initialize log file with header
		if new_log:
			if readonly:
				return False
			if not trywrite(self.fd, 'LOGGER  '):
				return False

		return True

	def close(self):
		if self.fd is None:
			return
		os.close(self.fd)
		self.fd = None

	def sync(self):
		try:
			os.fsync(self.fd)
		except OSError:
			return False
		return True

	def tableop(self, tablemeta, txn, delete=False):
		tr = TableRecord()
		tr.tabname = tablemeta.name
		if delete:
			dr.recmask |= LOGR_DELETE
		tr.root_id = tablemeta.root_id
		if txn is not None:
			tr.txn_id = txn.id

		if not writeobj(self.fd, tr):
			return None

		return tr

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

		if not writeobj(self.fd, dr):
			return None

		return dr

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
			os.lseek(self.fd, 0, os.SEEK_SET)
			hdr = tryread(self.fd, 8)
			if hdr is None or hdr != 'LOGGER  ':
				return False
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

		elif hdr == LOGR_ID_TABLE:
			obj = TableRecord()

		else:
			return None

		if not obj.deserialize(self.fd):
			return None
		return obj


