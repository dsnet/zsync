# ZFS syncing daemon #


## Introduction ##

The `zsync` daemon auto-snapshots ZFS datasets, replicates datasets, and
auto-deletes stale snapshots. The replication can be performed across machines
in either a push or pull mechanism using either SSH or local OS subprocesses.

This only tested with [ZFS on Linux](http://zfsonlinux.org/).


## Usage ##

### Delegate ZFS permissions ###

In order for the daemon to properly perform ZFS operations, the `zfs allow`
feature must be used to enable permissions on certain operations.

The following permissions should be granted:
```bash
# On all sources:
sudo zfs allow $USER send $DATASET

# On all mirrors:
sudo zfs allow $USER receive,create,mount,mountpoint,readonly $DATASET

# On all sources and mirrors:
sudo zfs allow $USER snapshot,destroy,mount $DATASET
```

The `mountpoint` and `readonly` properties are only need when performing the
first replication, where the receiving side sets `mountpoint=none` and
`readonly=on`.

The `zpool` and `zfs` tool must work without `sudo`.
See the ["Changes in Behavior" section of the ZoL v0.7.0 release](https://github.com/zfsonlinux/zfs/releases/tag/zfs-0.7.0).

### Build the daemon ###

```go get -u github.com/dsnet/zsync```

The instruction steps below assume `$GOPATH/bin` is in your `$PATH`,
so that the `zsync` binary can be executed directly from the shell.

### Create configuration file ###

The operation of `zsync` is configured using a JSON configuration file,
where the full list of features can be listed by running `zsync -help`.

Here is an example configuration file (adjust as appropriate):
```javascript
{
	"SSH": {
		"KeyFiles":       ["key.priv"],
		"KnownHostFiles": ["known_hosts"],
	},

	"AutoSnapshot": {"Cron": "@daily", "Count": 7},
	"Datasets": [{
		"Source":  "//localhost/tank/dataset",
		"Mirrors": ["//user@remotehost:22/tank/dataset-mirror"],
	}],
}
```

This example auto-snapshots daily and only keeps 7 snapshots.
It replicates `tank/dataset` on the local machine to `tank/dataset-mirror`
on a remote machine using an SSH push.

### Running the daemon ###

Start the daemon with:
```bash
zsync /path/to/config.json
```
