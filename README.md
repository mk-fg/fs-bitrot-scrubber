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

Ideally, user should only remember about it's existance when (and only when)
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

Under heavy development, not ready for any serious use yet.
