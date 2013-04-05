#!/usr/bin/env python
from __future__ import print_function

import itertools as it, operator as op, functools as ft
from os.path import realpath, join, isdir, dirname, basename, exists
from time import time, sleep
import os, sys, re, hashlib, stat, types, logging


try: from fs_bitrot_scrubber.db import MetaDB
except ImportError:
	# Make sure it works from a checkout
	if isdir(join(dirname(__file__), 'fs_bitrot_scrubber'))\
			and exists(join(dirname(__file__), 'setup.py')):
		sys.path.insert(0, dirname(__file__))
	from fs_bitrot_scrubber.db import MetaDB


is_str = lambda obj,s=types.StringTypes: isinstance(obj, s)

_re_type = type(re.compile(''))

def check_filters(path, filters, default=True):
	path = '/' + path
	for rule in filters:
		try: x, pat = rule
		except (TypeError, ValueError): x, pat = False, rule
		if not isinstance(pat, _re_type): pat = re.compile(pat)
		if pat.search(path): return x
	return default

def token_bucket(metric, spec):
	try:
		try: interval, burst = spec.rsplit(':', 1)
		except (ValueError, AttributeError): interval, burst = spec, 1.0
		else: burst = float(burst)
		if is_str(interval):
			try: a, b = interval.split('/', 1)
			except ValueError: interval = float(interval)
			else: interval = op.truediv(*it.imap(float, [a, b]))
		if min(interval, burst) < 0: raise ValueError()
	except:
		raise ValueError('Invalid format for rate limit (metric: {}): {!r}'.format(metric, spec))

	tokens, rate, ts_sync = burst, interval**-1, time()
	val = yield
	while True:
		ts = time()
		ts_sync, tokens = ts, min(burst, tokens + (ts - ts_sync) * rate)
		val, tokens = (None, tokens - val)\
			if tokens >= val else ((val - tokens) / rate, tokens - val)
		val = yield val



def file_list(paths, xdev=True, path_filter=list()):
	_check_filters = ft.partial(check_filters, filters=path_filter)
	paths = set(it.imap(realpath, paths))

	while paths:
		path_base = paths.pop()
		path_base_dev = os.stat(path_base).st_dev

		for p, dirs, files in os.walk(path_base, topdown=True):
			if xdev and p not in paths and os.stat(p).st_dev != path_base_dev: continue
			paths.discard(p)

			i_off = 0
			for i, name in list(enumerate(dirs)):
				path = join(p, name)
				# Filtered-out dirs won't be descended into
				if not _check_filters(path + '/'):
					del dirs[i - i_off]
					i_off += 1 # original list just became shorter

			for name in files:
				path = join(p, name)
				if not _check_filters(path): continue
				fstat = os.lstat(path)
				if not stat.S_ISREG(fstat.st_mode): continue
				yield path, fstat


def scrub( paths, meta_db,
		xdev=True, path_filter=list(),
		skip_for=3 * 3600, bs=4 * 2**20, rate_limits=None ):
	log = logging.getLogger('scrubber')
	log.debug('Scrub generation number: {}'.format(meta_db.generation))

	scan_limit = getattr(rate_limits, 'scan', None)
	read_limit = getattr(rate_limits, 'read', None)
	delay_ts = 0 # deadline for the next limit

	file_node = None # currently scrubbed (checksummed) file

	for path, fstat in file_list(paths, xdev=xdev, path_filter=path_filter):
		# Bumps generaton number on path as well, to facilitate cleanup
		meta_db.metadata_check( path,
			size=fstat.st_size, mtime=fstat.st_mtime, ctime=fstat.st_ctime )

		# Scan always comes first, unless hits the limit
		if not scan_limit: continue
		delay = scan_limit.send(1)
		if not delay: continue
		ts = time()
		if ts + delay < delay_ts: continue # reads are still banned
		delay_ts = ts + delay

		while True:
			if ts >= delay_ts: break

			if not file_node: # pick next node
				# TODO: check mtime after hashing to see if file changed since then
				file_node = meta_db.get_file_to_scrub(skip_for=skip_for)
			if not file_node: # nothing left/yet in this generation
				delay = max(0, delay_ts - ts)
				if delay:
					log.debug('Rate-limiting delay (scan): {:.1f}s'.format(delay))
					sleep(delay)
				break

			bs_read = file_node.read(bs)
			if not bs_read: # done with this one
				file_node.close()
				file_node = None
			ts = time()

			if read_limit: # min delay
				delay = read_limit.send(bs_read)
				if delay:
					if ts + delay >= delay_ts:
						delay_ts = ts + delay
						break # scan comes next
					else:
						log.debug('Rate-limiting delay (read): {:.1f}s'.format(delay))
						sleep(delay)
						ts = time()

	# Drop all meta-nodes for files with old generation
	meta_db.metadata_clean()

	# Check the rest of non-clean files in this gen
	while True:
		if not file_node: file_node = meta_db.get_file_to_scrub(skip_for=skip_for)
		if not file_node: break
		bs_read = file_node.read(bs)
		if not bs_read:
			file_node.close()
			file_node = None
		if read_limit:
			delay = read_limit.send(bs_read)
			if delay:
				log.debug('Rate-limiting delay (read): {:.1f}s'.format(delay))
				sleep(delay)



def main(argv=None):
	import argparse
	parser = argparse.ArgumentParser(
		description='Check integrity of at-rest files/data.')
	parser.add_argument('-c', '--config',
		action='append', metavar='path', default=list(),
		help='Configuration files to process.'
			' Can be specified more than once.'
			' Values from the latter ones override values in the former.'
			' Available CLI options override the values in any config.')
	parser.add_argument('--debug',
		action='store_true', help='Verbose operation mode.')
	optz = parser.parse_args(sys.argv[1:] if argv is None else argv)

	## Read configuration files
	from lya import AttrDict
	cfg = AttrDict.from_yaml('{}.yaml'.format(
		os.path.splitext(os.path.realpath(__file__))[0] ))
	for k in optz.config: cfg.update_yaml(k)

	## Logging
	logging.basicConfig(
		level=logging.WARNING if not optz.debug else logging.DEBUG )
	log = logging.getLogger()

	## Options processing
	try: cfg.operation.checksum = getattr(hashlib, cfg.operation.checksum)
	except AttributeError: cfg.operation.checksum = hashlib.new(cfg.operation.checksum)
	if is_str(cfg.storage.path): cfg.storage.path = [cfg.storage.path]
	_filter_actions = {'+': True, '-': False}
	cfg.storage.filter = list(
		(_filter_actions[pat[0]], re.compile(pat[1:]))
		for pat in (cfg.storage.filter or list()) )
	for metric, spec in cfg.operation.rate_limit.viewitems():
		if not spec: continue
		spec = token_bucket(metric, spec)
		next(spec)
		cfg.operation.rate_limit[metric] = spec
	if not cfg.storage.metadata.db_parity:
		cfg.storage.metadata.db_parity = cfg.storage.metadata.db + '.check'
	skip_for = cfg.operation.skip_for_hours * 3600
	cfg.operation.read_block = int(cfg.operation.read_block)

	## Actual work
	with MetaDB( cfg.storage.metadata.db,
			cfg.storage.metadata.db_parity, cfg.operation.checksum,
			log_queries=cfg.logging.sql_queries ) as meta_db:
		scrub( cfg.storage.path, meta_db,
			xdev=cfg.storage.xdev, path_filter=cfg.storage.filter,
			skip_for=skip_for, bs=cfg.operation.read_block, rate_limits=cfg.operation.rate_limit )

	log.debug('Done')


if __name__ == '__main__': sys.exit(main())
