fs-bitrot-scrubber
--------------------

A tool to detect userspace-visible changes to (supposedly) at-rest data on any
posix filesystem by scrubbing file contents.

Such unwanted changes are often referred to as "decay" or
[bitrot](http://en.wikipedia.org/wiki/Bit_rot#Decay_of_storage_media), but the
word may also refer to mostly unrelated concept of problematic legacy code in
software.

Most docs seem to refer to abscence of corruption as "integrity" (and
enforcement of this property as "integrity management"), but it's also heavily
tied to intentional corruption due to security breach ("tampering"), which is
completely out of scope for this tool.

Goal of the tool is to detect filesystem-backing layer (or rare kernel bug-)
induced corruption before it perpetuates itself into all existing backups (as
old ones are phased-out) and while it can still be reverted, producing as little
false-positives as possible and being very non-intrusive - don't require any
changes to existing storage stack and cooperate nicely with other running
software by limiting used storage bandwidth.

Ideally, user should only remember about its existance when (and only when)
there is a legitimate and harmful incident of aforementioned corruption.

Again, please note that this is *NOT* a security-oriented tool, as it doesn't
raise any alarms on any changes that have any signs of being intentional (like
updated mtime/ctime of the file) and doesn't enforce any policy on such changes.
Tools like [tripwire](http://sourceforge.net/projects/tripwire/) or one of the
kernel-level change detection mehanisms
([dm-verity](https://code.google.com/p/cryptsetup/wiki/DMVerity),
[IMA/EVM](http://linux-ima.sourceforge.net/), etc) are much more suited for that
purpose.

Tool also does not offer any protection against (apparently quite common) RAM
failures, which corrupt data that is (eventually) being written-back to disk, or
from anything that happens during file/data changes in general, focusing only on
"untouched" at-rest data.

Some studies/thoughts on the subject:

- [Schwarz et.al: Disk Scrubbing in Large, Archival Storage
	Systems](http://www.cse.scu.edu/~tschwarz/Papers/mascots04.pdf)

- [Baker et.al: A fresh look at the reliability of long-term digital
	storage](http://arxiv.org/pdf/cs/0508130)

- [Bairavasundaram et.al: An Analysis of Latent Sector Errors in Disk
	Drives](http://bnrg.eecs.berkeley.edu/~randy/Courses/CS294.F07/11.1.pdf)

- [KAHN Consulting: An Evaluation of EMC Centera Governance
	Edition](http://uk.emc.com/collateral/analyst-reports/kci-evaluation-of-emc-centera.pdf)

- [Jeff Bonwick: ZFS End-to-End Data
	Integrity](https://blogs.oracle.com/bonwick/entry/zfs_end_to_end_data)

- [My blog post with brief overview of the issue throughout linux storage
	stack](http://blog.fraggod.net/2013/04/06/fighting-storage-bitrot-and-decay.html)



Installation
--------------------

It is a regular package for Python 2.7 (not 3.X), but not in pypi, so can be
installed from a checkout with something like this:

	% python setup.py install

Better way would be to use [pip](http://pip-installer.org/) to install all the
necessary dependencies as well:

	% pip install 'git+https://github.com/mk-fg/fs-bitrot-scrubber.git#egg=fs-bitrot-scrubber'

Note that to install stuff in system-wide PATH and site-packages, elevated
privileges are often required.
Use "install --user",
[~/.pydistutils.cfg](http://docs.python.org/install/index.html#distutils-configuration-files)
or [virtualenv](http://pypi.python.org/pypi/virtualenv) to do unprivileged
installs into custom paths.

Alternatively, `./fs-bitrot-scrubber` can be run right from the checkout tree,
without any installation, provided all the necessary dependencies are in place.


### Requirements

* [Python 2.7 (not 3.X)](http://python.org) with sqlite3 support
* [layered-yaml-attrdict-config](https://github.com/mk-fg/layered-yaml-attrdict-config)



Usage
--------------------

Tool is intended to be run non-interactively by crond, anacron, systemd or
similar task scheduler.

First step would be to create configuration file (e.g. `/etc/fs-scrubber.yaml`),
for example:

	storage:
	  path: /srv/my-data
	  metadata:
	    db: /var/lib/fs-scrubber.sqlite

Configuration format is [YAML](https://en.wikipedia.org/wiki/YAML), all
available options are documented [in the shipped baseline
config](https://github.com/mk-fg/fs-bitrot-scrubber/blob/master/fs_bitrot_scrubber/core.yaml).

Then run the tool in "scrub" mode:

	% fs-bitrot-scrubber -c /etc/fs-scrubber.yaml scrub

It should produce no output (unless there are some access/permission issues),
record all files under `/srv/my-data` (as per example config above) in the
database (`/var/lib/fs-scrubber.sqlite`) along with their contents' checksums.

Repeated runs should also only produce stderr output on access issues or
legitimate at-rest data corruption - change in file contents' checksum without
any changes in its size, mtime or ctime.


### Status

Information on checked files can be queried using "status" command:

	% fs-bitrot-scrubber -c /etc/fs-scrubber.yaml status --verbose
	path: /srv/my-data/photo.jpg
	  checked: 2013-04-06 01:25:19.585862 (last run: True)
	  dirty: False

	path: /srv/my-data/videos/some_recording.avi
	  checked: 2013-04-06 01:25:20.233433 (last run: True)
	  dirty: False
	...

"dirty" there means "modification detected, but new checksum is not yet
calculated".
Use --help command-line flag to get more info on available options, commands and
their output.


### Logging

To get more information on what's happening under the hood, during long
operations, use "--debug" flag or (much better) setup proper logging, via
"logging" section in the configuration file, e.g.:

	logging:
	  handlers:
	    debug_logfile:
	      class: logging.handlers.RotatingFileHandler
	      filename: /var/log/fs-scrubber/debug.log
	      formatter: basic
	      encoding: utf-8
	      maxBytes: 5_242_880 # 5 MiB
	      backupCount: 2
	      level: DEBUG
	  root:
	    level: DEBUG
	    handlers: [console, debug_logfile]

That way, noisy debug-level logging and status updates can be tracked via
configured logfile, while stderr will still only contain actionable errors and
no other noise.

Note that DEBUG logging relates to number of processed paths via O(n), but all
less-common messages (e.g. skipping a mountpoint, change to contents + mtime,
etc) are INFO, and above if message is actionable (e.g. corruption).

See default [python logging subsystem
documentation](http://docs.python.org/library/logging.config.html) for more
details on the concepts involved, other avalable handlers (syslog, network, etc)
and the general format of the section above.


### Rate limiting

See "operation.rate_limit" [config
file](https://github.com/mk-fg/fs-bitrot-scrubber/blob/master/fs_bitrot_scrubber/core.yaml)
section for details.

Simple configurable [token bucket](https://en.wikipedia.org/wiki/Token_Bucket)
algorithm is used at this point.

Example configuration might look like:

	operation:
	  rate_limit:
	    scan: 1/10:30     # scan *at most* 10 files per second, with up to 30 file bursts
	    read: 1/3e6:30e6  # read *at most* ~3 MiB/s with up to 30 MiB bursts

Default configuration has no limits configured.


### Checksum algorithm

[SHA-256](http://en.wikipedia.org/wiki/Sha256) (SHA-2 family hash) is configured
by default, but can be changed via "operation.checksum" key to whatever [python
hashlib](http://docs.python.org/2/library/hashlib.html) has support for
(generally everything that's included in openssl), for example:

	operation:
	  checksum: ripemd160

All the files will have to be rehashed on any change to this parameter and
metadata db may as well be just removed in such case, so that tool won't report
a lot of checksum mismatches on the next run.


### Filtering

Fairly powerful filtering akin to rsync ordered filter-lists is available:

	storage:
	  filter:
	    - '+^/srv/video/($|family/)'  # check/scrub all files in specified subpath
	    - '-^/srv/video/'             # *don't* check any other videos there
	    - '-^/srv/images/cat-gifs/'   # there's plenty of these on the internets
	    - '-(?i)\.(mp3|ogg)$'         # skip mp3/ogg files in any path, unless included above

Note that the first regexp matches "/srv/video/" as well, so that the path
itself will be traversed, even though all contents other than "family" subpath
will be ignored.

`(?i)` thing in the last regexp is a modifier, as per [python regexp
syntax](http://docs.python.org/2/library/re.html#regular-expression-syntax).
See more info on filters in the [base
config](https://github.com/mk-fg/fs-bitrot-scrubber/blob/master/fs_bitrot_scrubber/core.yaml).


### Interrupt / resume

Scrubbing operation can always be resumed if interrupted.

If it was interrupted during scan (crawling over fs tree) though, running "scrub
--resume" will only check paths that were seen during last scan, which might be
some subset of all the ones available in the paths and/or discovered during
previous scan.

If scan and checksum operations should not be interleaved, "scrub --scan-only"
can be run first to update the file list, followed by "scrub --resume" to
actually check these files.



Plans
--------------------

- Add sane, robust and behind-the-scenes parity check/restore for at least
	metadata db (or maybe also filtered list of files) via
	[zfec](https://tahoe-lafs.org/trac/zfec) or similar module.

- More dynamic rate-limiting options - query
	[sysstat](http://sebastien.godard.pagesperso-orange.fr/) or similar system for
	disk load and scale up/down depending on that.

	Though maybe blkio cgroup resource controller and ionice settings should
	suffice here.

- Use fcntl leases, inotify or some other non-polling mechanism to reliably
	detect changes from other pids during file checksumming.

- Better progress logging - should be easy to display how much files and even
	GiBs is left to check on a single run.
	Might also be worth logging performance stats.
