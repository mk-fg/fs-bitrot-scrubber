#!/usr/bin/env python

import itertools as it, operator as op, functools as ft
import os, sys


# try: from fs_bitrot_scrubber import db
# except ImportError:
# 	# Make sure it works from a checkout
# 	if isdir(join(dirname(__file__), 'fs_bitrot_scrubber'))\
# 			and exists(join(dirname(__file__), 'setup.py')):
# 		sys.path.insert(0, dirname(__file__))
# 	from fs_bitrot_scrubber import db


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
	import logging
	logging.basicConfig(
		level=logging.WARNING if not optz.debug else logging.DEBUG )
	log = logging.getLogger()

	raise NotImplementedError()

	log.debug('Done')


if __name__ == '__main__': sys.exit(main())
