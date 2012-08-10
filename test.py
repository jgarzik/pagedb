#!/usr/bin/python

import sys

import PageDb

DBDIR='/tmp/dbdir'
DBTABLE='test1'


datadict = {
	'name' : 'jeff',
	'age' : '38',
	'faith' : 'yes',
}


def test1():
	db = PageDb.PageDb()
	if not db.create(DBDIR):
		print "create failed"
		sys.exit(1)

	if not db.create_table(DBTABLE):
		print "create table failed"
		sys.exit(1)

	table = db.open_table(DBTABLE)
	if table is None:
		print "open table failed"
		sys.exit(1)

	txn = db.txn_begin()
	if txn is None:
		print "txn begin failed"
		sys.exit(1)

	for k, v in datadict.iteritems():
		if not table.put(txn, k, v):
			print "put", k, "failed"
			sys.exit(1)

	if not db.txn_commit(txn):
		print "txn commit failed"
		sys.exit(1)

	for k, v in datadict.iteritems():
		dbv = table.get(None, k)
		if v != dbv:
			print "key mismatch for:", k

	for k in datadict.iterkeys():
		ok = table.exists(None, k)
		if not ok:
			print "key not found for:", k

test1()

sys.exit(0)

