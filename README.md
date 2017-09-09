# ZFS syncing daemon #

## Introduction ##

The zsync daemon auto-snapshots datasets, transfers ZFS datasets
across multiple machines in either a push or pull mechanism using either SSH or
local-OS subprocesses, and also auto-deletes stale snapshots.
