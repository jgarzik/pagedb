
import os
import mmap
import random

class Page(object):
	def __init__(self):
		self.idx = None
		self.map = None

	def flush(self):
		self.map.flush()

class PageFile(object):
	def __init__(self):
		self.fd = None
		self.readonly = True
		self.pagesz = 1 * 1024 * 1024
		self.n_pages = 0
		self.n_cache = 100
		self.pages = {}
	
	def __del__(self):
		if self.fd is not None:
			self.pages = {}
			os.close(self.fd)

	def open(self, filename, readonly=False):
		self.readonly = readonly
		if readonly:
			open_flags = os.O_RDONLY
		else:
			open_flags = os.O_RDWR | os.O_CREAT

		try:
			self.fd = os.open(filename, open_flags, 0666)
			stat = os.fstat(self.fd)
		except OSError:
			return False

		self.n_pages = stat.st_size / self.pagesz
		if (self.n_pages * self.pagesz) != stat.st_size:
			return False

		return True
		
	def close(self):
		for k, v in self.pages.iteritems():
			v.flush()
		self.pages = {}

		try:
			os.fsync(self.fd)
			os.close(self.fd)
		except OSError:
			pass
		self.fd = None

		self.n_pages = 0

	def getpage(self, n):
		if n >= self.n_pages:
			return None
		if n in self.pages:
			return self.pages[n]

		if len(self.pages) >= self.n_cache:
			self.shrink_cache()
		
		if self.readonly:
			prot_flags = mmap.PROT_READ
		else:
			prot_flags = mmap.PROT_READ | mmap.PROT_WRITE

		page = Page()
		page.idx = n
		page.map = mmap.mmap(self.fd, self.pagesz, os.MAP_SHARED,
				     self.prot_flags, None,
				     n * self.pagesz)

		self.pages[n] = page

		return page
	
	def shrink_cache(self):
		keys = self.pages.keys()
		random.shuffle(keys)

		while len(self.pages) >= self.n_cache:
			key = keys.pop()
			del self.pages[key]

	def expand(self, count=1):
		new_pages = self.n_pages + count
		new_size = new_pages * self.pagesz

		try:
			os.ftruncate(self.fd, new_size)
		except OSError:
			return False

		self.n_pages = new_pages

		return True

	def newpage(self):
		if not self.expand():
			return None
		return self.getpage(self.n_pages - 1)

