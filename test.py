#!/usr/bin/python
#
# Copyright 2012 Red Hat, Inc.
#
# Distributed under the MIT/X11 software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.
#

import sys
import shutil
import os

import PageDb

DBDIR='/tmp/dbdir'
DBTABLE='test1'


datadict = {
	'name' : 'jeff',
	'age' : '38',
	'faith' : 'yes',
	'barnyard' : 'chickens',
	'goose' : 'egg',
}

deleted_keys = { 'barnyard', 'goose' }

never_existed = { 'biff129', 'biff122' }


def prep():
	shutil.rmtree(DBDIR)
	os.mkdir(DBDIR)

def test1(test_iter):
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

	txn = db.txn_begin()
	if txn is None:
		print "txn2 begin failed"
		sys.exit(1)

	for k in deleted_keys:
		if not table.delete(txn, k):
			print "del", k, "failed"
			sys.exit(1)

	if not db.txn_commit(txn):
		print "txn2 commit failed"
		sys.exit(1)

	for k in deleted_keys:
		dbv = table.get(None, k)
		if dbv is not None:
			print "key still get's for:", k

	for k in deleted_keys:
		ok = table.exists(None, k)
		if ok:
			print "key still exists for:", k

	for k in never_existed:
		dbv = table.get(None, k)
		if dbv is not None:
			print "neverexisted key get's for:", k

	for k in never_existed:
		ok = table.exists(None, k)
		if ok:
			print "neverexisted key exists for:", k

	print "test%d OK" % (test_iter,)

def test2(test_iter):
	db = PageDb.PageDb()
	if not db.open(DBDIR):
		print "open failed"
		sys.exit(1)

	table = db.open_table(DBTABLE)
	if table is None:
		print "open table failed"
		sys.exit(1)

	for k, v in datadict.iteritems():
		dbv = table.get(None, k)
		if k in deleted_keys:
			if dbv is not None:
				print "key still get's for:", k
		else:
			if v != dbv:
				print "key mismatch for:", k

	for k in datadict.iterkeys():
		ok = table.exists(None, k)
		if k in deleted_keys:
			if ok:
				print "key still exists for:", k
		else:
			if not ok:
				print "key not found for:", k

	for k in never_existed:
		dbv = table.get(None, k)
		if dbv is not None:
			print "neverexisted key get's for:", k

	for k in never_existed:
		ok = table.exists(None, k)
		if ok:
			print "neverexisted key exists for:", k

	print "test%d OK" % (test_iter,)

def test3(test_iter):
	db = PageDb.PageDb()
	if not db.open(DBDIR):
		print "open failed"
		sys.exit(1)

	if not db.checkpoint():
		print "checkpoint failed"
		sys.exit(1)

	print "test%d OK" % (test_iter,)

prep()
test1(1)
test2(2)
test3(3)
test2(4)

sys.exit(0)

