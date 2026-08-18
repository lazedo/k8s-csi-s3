# Writing through csi-s3 without losing data

A csi-s3 volume is a userspace filesystem: the mounter (geesefs by default)
can die — OOM, crash, node pressure — and until v1.34.8 the mountpoint stayed
dead forever ("Socket not connected"). Two layers address this; writers that
cannot afford to lose data (call recordings, faxes, media capture) should use
both.

## Layer 1 — the driver heals the mount (v1.34.8+)

The node plugin supervises every staged volume (`CSI_S3_SUPERVISE_INTERVAL`,
default 30s, 0 disables): a statfs probe returning `ENOTCONN`/`EIO` marks the
endpoint dead; the supervisor lazily detaches the publish binds and the
staging mount, remounts with the exact NodeStage parameters (COSI-handle
volumes re-resolve their credentials in-cluster), and re-binds the publish
targets. State survives plugin restarts (`supervisor-state.json` in the
plugin dir).

Propagation caveat: a container that mounted the volume with the default
`mountPropagation: None` captured the dead superblock and only sees the
healed mount after a container restart. **Consumers that want live healing
must mount with `mountPropagation: HostToContainer`.**

## Layer 2 — the writer spools locally (the pattern)

Never write in-progress files straight to the FUSE mount. Instead:

1. **Spool**: write active files to node-local storage (`emptyDir`), e.g.
   `/var/spool/<svc>/`. Local writes never block on S3 and survive an outage
   window.
2. **Finish**: when a file completes (capture closed, recording ended), move
   it to the volume: copy to `<vol>/.incoming-<name>`, fsync, rename to the
   final name (rename within the volume is atomic enough for readers that
   ignore dotfiles), then unlink the spool copy.
3. **Drain**: a loop (30s) retries the finished-but-unmoved backlog oldest
   first. "Recovered" needs no signal: the probe IS the operation — statfs on
   the volume plus the first successful copy. With Layer 1 + HostToContainer
   the mount comes back by itself within a supervision interval.
4. **Bound the spool**: cap by bytes and age; over the cap, drop the oldest
   finished files loudly (metric + log). Full local disk kills the node —
   dropping capture is the cheaper failure.

The result: an S3/mounter outage costs nothing while it is shorter than the
spool cap, and the writer never blocks on the mount.

First consumers: media-capture (PCAP), next call recordings and faxes.
