#-*- coding: utf-8 -*-

# /usr/include/linux/fadvise.h
POSIX_FADV_SEQUENTIAL = 2
POSIX_FADV_DONTNEED = 4

libc = offset = length = None

def fadvise(fd, seq=False, drop_cache=False):
	'Avoid filling disk cache with discardable data.'
	global libc, offset, length
	if not libc: # only import and initialize ctypes if used
		import ctypes, ctypes.util
		libc = ctypes.CDLL(ctypes.util.find_library('c'))
		offset = length = ctypes.c_uint64(0)
	if not isinstance(fd, (int, long)): fd = fd.fileno()

	# These don't work (EINVAL) when or'ed
	if seq: libc.posix_fadvise(fd, offset, length, POSIX_FADV_SEQUENTIAL)
	if drop_cache: libc.posix_fadvise(fd, offset, length, POSIX_FADV_DONTNEED)
