#!/usr/bin/env python
#-*- coding: utf-8 -*-
from __future__ import print_function

import itertools as it, operator as op, functools as ft
from os.path import realpath, join, isdir, dirname, basename, exists
from contextlib import contextmanager
from time import time, sleep
import os, sys, re, hashlib, stat, types, logging


try: from fs_bitrot_scrubber import db, force_unicode
except ImportError:
	# Make sure it works from a checkout
	if isdir(join(dirname(__file__), 'fs_bitrot_scrubber'))\
			and exists(join(dirname(__file__), 'setup.py')):
		sys.path.insert(0, dirname(__file__))
	from fs_bitrot_scrubber import db, force_unicode


is_str = lambda obj,s=types.StringTypes: isinstance(obj, s)

_re_type = type(re.compile(''))

def check_filters(path, filters, default=True):
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
	log = logging.getLogger('bitrot_scrubber.walk')

	while paths:
		path_base = paths.pop()
		try: path_base_dev = os.stat(path_base).st_dev
		except (OSError, IOError):
			log.info(force_unicode('Unable to access scrub-path: {}'.format(path_base)))
			continue

		for p, dirs, files in os.walk(path_base, topdown=True):
			if xdev and p not in paths and os.stat(p).st_dev != path_base_dev:
				log.info(force_unicode('Skipping mountpoint: {}'.format(p)))
				while dirs: dirs.pop() # don't descend into anything here
				continue
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
		xdev=True, path_filter=list(), scan_only=False,
		skip_for=3 * 3600, bs=4 * 2**20, rate_limits=None ):
	log = logging.getLogger('bitrot_scrubber.scrub')
	log.info('Scrub generation number: {}'.format(meta_db.generation))

	scan_limit = getattr(rate_limits, 'scan', None)
	if not scan_only: read_limit = getattr(rate_limits, 'read', None)
	ts_scan = ts_read = 0 # deadline for the next iteration

	file_node = None # currently scrubbed (checksummed) file

	for path, fstat in file_list(paths, xdev=xdev, path_filter=path_filter):
		log.debug(force_unicode('Scanning path: {}'.format(path)))
		# Bumps generaton number on path as well, to facilitate cleanup
		meta_db.metadata_check( path,
			size=fstat.st_size, mtime=fstat.st_mtime, ctime=fstat.st_ctime )

		# Scan always comes first, unless hits the limit
		if not scan_limit: continue
		ts, delay = time(), scan_limit.send(1)
		if not delay: continue
		ts_scan = ts + delay

		while True:
			if ts >= ts_scan: break # get back to scan asap

			if not scan_only and not file_node: # pick next node
				file_node = meta_db.get_file_to_scrub(skip_for=skip_for)
			if ts_scan < ts_read or not file_node:
				delay = ts_scan - ts
				if delay > 0:
					# log.debug('Rate-limiting delay (scan): {:.1f}s'.format(delay))
					sleep(delay)
				break

			bs_read = file_node.read(bs)
			if not bs_read: # done with this one
				file_node.close()
				file_node = None
			ts = time()

			if read_limit:
				delay = read_limit.send(bs_read)
				if delay:
					ts_read = ts + delay
					if ts_read < ts_scan:
						# log.debug('Rate-limiting delay (read): {:.1f}s'.format(delay))
						sleep(delay)
						ts = time()

	# Drop all meta-nodes for files with old generation
	meta_db.metadata_clean()
	if scan_only: return

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
				# log.debug('Rate-limiting delay (read): {:.1f}s'.format(delay))
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

	cmds = parser.add_subparsers(
		title='Supported operations (have their own suboptions as well)')

	@contextmanager
	def subcommand(name, **kwz):
		cmd = cmds.add_parser(name, **kwz)
		cmd.set_defaults(call=name)
		yield cmd

	with subcommand('scrub', help='Scrub configured paths, detecting bitrot,'
			' updating checksums on legitimate changes and adding new files.') as cmd:
		cmd.add_argument('-s', '--scan-only', action='store_true',
			help='Do not process file contents (or open'
				' them) in any way, just scan for new/modified files.')
		cmd.add_argument('-p', '--extra-paths', nargs='+', metavar='path',
			help='Extra paths to append to the one(s) configured via "storage.path".'
				' Can be used to set the list of paths dynamically (e.g., via wildcard from shell).')

	with subcommand('status', help='List files with status recorded in the database.') as cmd:
		cmd.add_argument('-v', '--verbose', action='store_true',
			help='Display last check and modification info along with the path.')
		cmd.add_argument('-d', '--dirty', action='store_true',
			help='Only list files which are known to be modified since last checksum update.')
		cmd.add_argument('-c', '--checked', action='store_true',
			help='Only list files which were checked on the last run.')
		cmd.add_argument('-u', '--not-checked', action='store_true',
			help='Only list files which were left unchecked on the last run.')
		# cmd.add_argument('-n', '--new', action='store_true',
		# 	help='Files that are not yet recorded at all, but exist on disk. Implies fs scan.')

	optz = parser.parse_args(sys.argv[1:] if argv is None else argv)

	## Read configuration files
	import lya
	cfg = lya.AttrDict.from_yaml('{}.yaml'.format(
		os.path.splitext(os.path.realpath(__file__))[0] ))
	for k in optz.config: cfg.update_yaml(k)
	lya.configure_logging( cfg.logging,
		logging.WARNING if not optz.debug else logging.DEBUG )
	log = logging.getLogger('bitrot_scrubber.root')

	## Options processing
	if not cfg.storage.metadata.db:
		parser.error('Path to metadata db ("storage.metadata.db") must be configured.')
	try: cfg.operation.checksum = getattr(hashlib, cfg.operation.checksum)
	except AttributeError: cfg.operation.checksum = hashlib.new(cfg.operation.checksum)
	if is_str(cfg.storage.path): cfg.storage.path = [cfg.storage.path]
	else: cfg.storage.path = list(cfg.storage.path or list())
	_filter_actions = {'+': True, '-': False}
	cfg.storage.filter = list(
		(_filter_actions[pat[0]], re.compile(pat[1:]))
		for pat in (cfg.storage.filter or list()) )
	for metric, spec in cfg.operation.rate_limit.viewitems():
		if not spec: continue
		spec = token_bucket(metric, spec)
		next(spec)
		cfg.operation.rate_limit[metric] = spec
	if cfg.storage.metadata.db_parity is None:
		cfg.storage.metadata.db_parity = cfg.storage.metadata.db + '.check'
	skip_for = cfg.operation.skip_for_hours * 3600
	cfg.operation.read_block = int(cfg.operation.read_block)

	## Actual work
	log.debug('Starting (operation: {})'.format(optz.call))
	with db.MetaDB( cfg.storage.metadata.db,
			cfg.storage.metadata.db_parity, cfg.operation.checksum,
			log_queries=cfg.logging.sql_queries ) as meta_db:
		if optz.call == 'scrub':
			if optz.extra_paths: cfg.storage.path.extend(optz.extra_paths)
			if not cfg.storage.path:
				parser.error( 'At least one path to scrub must'
					' be specified (via "storage.path" in config or on commandline).' )
			scrub( cfg.storage.path, meta_db, scan_only=optz.scan_only,
				xdev=cfg.storage.xdev, path_filter=cfg.storage.filter,
				skip_for=skip_for, bs=cfg.operation.read_block, rate_limits=cfg.operation.rate_limit )

		elif optz.call == 'status':
			first_row = True
			for info in meta_db.list_paths():
				if optz.dirty and not info['dirty']: continue
				if optz.not_checked and info['clean']: continue
				if optz.checked and not info['clean']: continue

				if not optz.verbose: print(info['path'])
				else:
					if not first_row: print()
					else: first_row = False
					print('path: {}'.format(info['path']))
					print('  checked: {0[last_scrub]} (last run: {0[clean]})\n  dirty: {0[dirty]}{1}'.format(
						info, ', skipped: {}'.format(info['last_skip']) if info['last_skip'] else '' ))

		else: raise ValueError('Unknown command: {}'.format(optz.call))
	log.debug('Done')


if __name__ == '__main__': sys.exit(main())
