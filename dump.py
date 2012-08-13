#!/usr/bin/python

import os
import sys
import struct

import TableRoot, PageDb, PDcodec_pb2, Block
from util import tryread, readrec


def dblock(fd):
	while True:
		fpos = os.lseek(fd, 0, os.SEEK_CUR)
		tup = readrec(fd)
		if tup is None:
			return True

		recname = tup[0]
		data = tup[1]

		recstr = "%s(%d)" % (recname, fpos)

		if recname == 'DATA':
			hdr = data[:8]
			data = data[8:]

			blkent = Block.BlockEnt()
			blkent.deserialize_hdr(hdr)

			fmt = "%ds%ds" % (blkent.k_len, blkent.v_len)
			(blkent.k, blkent.v) = struct.unpack(fmt, data)

			print recstr, blkent.k_len, blkent.v_len
			print blkent.k
			print blkent.v, "\n"

		elif recname == 'DTRL':
			(arrpos, n_vals) = struct.unpack('<II', data)
			print recstr, arrpos, n_vals

		elif recname == 'DIDX':
			print recstr
			while len(data) >= 8:
				rec = data[:8]
				data = data[8:]
				(entpos, k_len) = struct.unpack('<II', rec)
				print entpos, k_len
			if len(data) > 0:
				print len(data), "bytes trailing"
			print ""

		else:
			print recstr


def dsuper(fd):
	tup = readrec(fd)
	if tup is None:
		print "Superblock deser failed"
		return False

	recname = tup[0]
	data = tup[1]

	if recname != 'SUPR':
		print "Superblock magic failed"
		return False

	obj = PDcodec_pb2.Superblock()
	try:
		obj.ParseFromString(data)
	except google.protobuf.message.DecodeError:
		print "Superblock deser failed 2"
		return False

	print str(obj)

	return True

def dtableroot(fd):
	os.lseek(fd, 0, os.SEEK_SET)

	tr = TableRoot.TableRoot(None, None)
	if not tr.deserialize(fd):
		print "TableRoot deser failed"
		return False

	for ent in tr.v:
		print "%d %s" % (ent.file_id, ent.key)

	return True

def dlogger(fd):
	while True:
		tup = readrec(fd)
		if tup is None:
			return True

		recname = tup[0]
		data = tup[1]

		print recname

def dumpfile(filename):
	fd = os.open(filename, os.O_RDONLY)

	magic = tryread(fd, 8)
	if magic is None:
		return False

	if magic == 'BLOCK   ':
		return dblock(fd)

	elif magic == 'LOGGER  ':
		return dlogger(fd)

	elif magic == 'TABLROOT':
		return dtableroot(fd)

	elif magic == 'SUPER   ':
		return dsuper(fd)

	return False


if __name__ == '__main__':
	if len(sys.argv) != 2:
		print "Usage: dump.py FILE"
		sys.exit(1)

	if not dumpfile(sys.argv[1]):
		print "dump failed"
		sys.exit(1)

	sys.exit(0)

