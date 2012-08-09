

import os


def tryread(fd, n):
	try:
		data = os.read(fd, n)
	except OSError:
		return None
	if len(data) != n:
		return None
	return data

