#!/usr/bin/env python

from setuptools import setup, find_packages
import os

pkg_root = os.path.dirname(__file__)

# Error-handling here is to allow package to be built w/o README included
try: readme = open(os.path.join(pkg_root, 'README.md')).read()
except IOError: readme = ''

setup(

	name = 'fs-bitrot-scrubber',
	version = '13.04.0',
	author = 'Mike Kazantsev',
	author_email = 'mk.fraggod@gmail.com',
	license = 'WTFPL',
	keywords = [ 'filesystem', 'fs', 'bitrot', 'corruption', 'change', 'detection',
		'backup', 'alteration', 'scrubbing', 'scrub', 'integrity', 'decay', 'bit rot',
		'storage', 'data', 'at-rest', 'at rest', 'disk', 'failure', 'control', 'checksum' ],
	url = 'http://github.com/mk-fg/fs-bitrot-scrubber',

	description = 'Tool to detect userspace-visible changes to'
		' (supposedly) at-rest data on any posix filesystem by scrubbing file contents',
	long_description = readme,

	classifiers = [
		'Development Status :: 4 - Beta',
		'Environment :: Console',
		'Environment :: No Input/Output (Daemon)',
		'Intended Audience :: Developers',
		'Intended Audience :: End Users/Desktop',
		'Intended Audience :: System Administrators',
		'License :: OSI Approved',
		'Operating System :: POSIX',
		'Programming Language :: Python',
		'Programming Language :: Python :: 2.7',
		'Programming Language :: Python :: 2 :: Only',
		'Topic :: Utilities' ],

	install_requires = ['layered-yaml-attrdict-config'],

	packages = find_packages(),
	include_package_data = True,
	zip_safe = False,

	package_data = {'fs_bitrot_scrubber': ['core.yaml']},
	entry_points = dict(console_scripts=[
		'fs-bitrot-scrubber = fs_bitrot_scrubber.core:main' ]) )
