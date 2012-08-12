
#
# Copyright 2012 Red Hat, Inc.
#
# Distributed under the MIT/X11 software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.
#

import os
import struct
import zlib
import google.protobuf

import PDcodec_pb2
from util import writepb, tryread, trywrite, readrec


LOGR_ID_DATA = 'LOGR'
LOGR_ID_TXN_START = 'TXN '
LOGR_ID_TXN_COMMIT = 'TXNC'
LOGR_ID_TXN_ABORT = 'TXNA'
LOGR_ID_TABLE = 'LTBL'
LOGR_DELETE = (1 << 0)


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
				self.fd = os.open(self.dbdir + name,
						  os.O_RDONLY)
			else:
				self.fd = os.open(self.dbdir + name,
						  os.O_CREAT | os.O_RDWR, 0666)
			st = os.fstat(self.fd)
			new_log = (st.st_size == 0)
			os.lseek(self.fd, 0, os.SEEK_END)
		except (OSError) as (errno, strerror):
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
		tr = PDcodec_pb2.LogTable()
		tr.tabname = tablemeta.name
		if txn is None:
			tr.txn_id = 0
		else:
			tr.txn_id = txn.id
		tr.recmask = 0
		if delete:
			tr.recmask |= LOGR_DELETE
		tr.root_id = tablemeta.root_id

		if not writepb(self.fd, LOGR_ID_TABLE, tr):
			return None

		return tr

	def data(self, tablemeta, txn, k, v, delete=False):
		dr = PDcodec_pb2.LogData()
		dr.table = tablemeta.name
		dr.txn_id = txn.id
		dr.recmask = 0
		if delete:
			dr.recmask |= LOGR_DELETE
		dr.key = k
		if not delete:
			dr.value = v

		if not writepb(self.fd, LOGR_ID_DATA, dr):
			return None

		return dr

	def txn_begin(self, txn):
		r = PDcodec_pb2.LogTxnOp()
		r.txn_id = txn.id

		return writepb(self.fd, LOGR_ID_TXN_START, r)

	def txn_end(self, txn, commit):
		r = PDcodec_pb2.LogTxnOp()
		r.txn_id = txn.id
		if commit:
			op = LOGR_ID_TXN_COMMIT
		else:
			op = LOGR_ID_TXN_ABORT
		return writepb(self.fd, op, r)

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
		tup = readrec(self.fd)
		if tup is None:
			return None
		recname = tup[0]
		data = tup[1]

		if recname == LOGR_ID_DATA:
			obj = PDcodec_pb2.LogData()

		elif (recname == LOGR_ID_TXN_START or
		      recname == LOGR_ID_TXN_COMMIT or
		      recname == LOGR_ID_TXN_ABORT):
			obj = PDcodec_pb2.LogTxnOp()

		elif recname == LOGR_ID_TABLE:
			obj = PDcodec_pb2.LogTable()

		else:
			return None

		try:
			obj.ParseFromString(data)
		except google.protobuf.message.DecodeError:
			return None

		return (recname, obj)


