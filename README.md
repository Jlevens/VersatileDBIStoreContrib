# VersatileStoreContrib
This is a flexible Fowsiki Store using a database backend for storage.

Attachments are managed by a different Store. The Foswiki::Store architecture allows for this in it's design explicitly.

PlainFileStore was used extensively during the development of VersatileStore as the attachment backend. It is this backend that is tested and recommended.

The existing RCS Stores **should** work but testing needs to be done to ensure these combinations work well.

## TODO

- [ ] Back out ConfigHash support i.e. $this->{cfg} back to $Foswiki::cfg, yuck!
* http://foswiki.org/Development/StoresShouldBePassedConfigHash for the gory details
- [ ] Pseudo-install into latest FW 2.0 and get up and running - basic testing, it needs more work anyway
- [ ] Document any further issues to deal with
- [ ] Copy and rename to VersatileStoreContrib with corresponding code changes
- [ ] Pseudo-install this version into latest FW 2.0 and get up and running - basic testing that the rename is OK
- [ ] Push this into the FW Repo to be the 'origin' version of this software, my version will be my fork for hacking and extra backup

See also: https://github.com/Jlevens/StoreToolsContrib
