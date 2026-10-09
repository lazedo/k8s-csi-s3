# Recovering dead FUSE mounts

A csi-s3 volume is a userspace filesystem: the mounter (geesefs by default)
is a daemon that can die (OOM, crash, node pressure). When it dies the
mountpoint stays in the mount table as a dead endpoint — every access returns
`ENOTCONN` ("Transport endpoint is not connected" / "Socket not connected") —
and stock csi-s3 never remounts it. There are three recovery layers, from
automatic to surgical.

## Layer 1 — the node supervisor heals the host mount (v1.34.8+)

The node plugin runs a supervisor loop (`CSI_S3_SUPERVISE_INTERVAL`, default
30s, 0 disables):

- **State**: every NodeStage/NodePublish is recorded (volumeID, staging path,
  volume context, stage secrets, publish targets) in `supervisor-state.json`
  under the container-side plugin dir (`/csi`), so it survives plugin
  restarts.
- **Discovery**: on startup and every 10th cycle it scans `/proc/mounts` for
  this driver's FUSE mounts, reads the `volumeHandle` from kubelet's
  `vol_data.json` next to each mountpoint, resolves the mount parameters from
  the PV (`volumeAttributes` + `nodeStageSecretRef`; node ClusterRole needs
  `persistentvolumes` get/list), and adopts mounts it did not stage itself —
  e.g. volumes staged by an older plugin version. This is why the driver can
  heal a mount that predates the supervisor.
- **Heal**: a `statfs` probe returning `ENOTCONN`/`EIO` marks the endpoint
  dead; the supervisor lazily detaches the publish binds and the staging
  mount, remounts the staging path with the recorded parameters (COSI-handle
  volumes re-resolve their credentials in-cluster), and re-binds the publish
  targets.

This fully repairs the **host-side** mount.

## Layer 2 — propagation carries the heal into running containers

Whether a *running* container sees the Layer-1 heal depends on its
`mountPropagation`:

- **`HostToContainer`** (`rslave`): mounts the host makes on top of the
  captured mount propagate into the running container live — but Layer 1's
  detach-and-rebind is not one of those (see below).
- **`None`** (the default, private): the container captured the mount at
  start and never sees a host remount. New pods / restarts are fine (they get
  the healthy mount); the already-running container stays broken. For those,
  Layer 3.

Measured 2026-10-09 (the lab below): Layer 1's heal — detach the dead binds,
remount, bind again at the same paths — reaches **no** running container,
`HostToContainer` included: each keeps the dead mount it captured. Layer 3
covers every container whatever its propagation.

## Layer 3 — the supervisor grafts the heal into running containers (no restart)

After a heal (Layer 1, or the NodeStage/NodePublish paths that remount a dead
staging), the supervisor remembers the dead superblock's device (`dead` in
`supervisor-state.json`), reads `/proc/*/mountinfo` of the node's processes —
the node plugin runs with **`hostPID: true`** — and, for every mount namespace
other than the node's and its own that still holds that device, grafts the
healed staging mount (plus the mount's subPath root) over each such mount
point with `/usr/bin/nsmount-heal`, keeping its `ro`/`nosuid`/`nodev`/`noexec`
flags. A graft that fails is retried every interval; the device is forgotten
once no container holds it. A restarted plugin heals at startup, not one
interval later. Code: `pkg/driver/graft.go`.

`cmd/nsmount-heal` (C — `setns(CLONE_NEWNS)` rejects a multithreaded caller,
so this cannot be Go; built static into the image) grafts a healthy mount
subtree onto the dead path *inside a running container's mount namespace*:

    nsmount-heal <container-pid> <source-path> <container-target-path> [ro,nosuid,nodev,noexec]

Mechanism:

1. `open_tree(OPEN_TREE_CLONE|AT_RECURSIVE)` clones the healthy source into a
   **detached** mount fd. The caller's own namespace must see the healthy
   source — the supervisor mounted it itself; by hand, run it from a helper
   pod that bind-mounts `/var/lib/kubelet` with `HostToContainer`, so the
   freshly-remounted staging tree propagates in.
2. `mount_setattr` gives the clone the flags of the mount it replaces.
3. `setns(container mnt ns)` — the only namespace switch, single-threaded.
4. `umount2(target, MNT_DETACH)` drops the corpse, `move_mount(cloneFD, "",
   target, MOVE_MOUNT_F_EMPTY_PATH)` grafts the clone over it.

By hand (the supervisor does all of this itself), from a helper pod
(privileged + hostPID, pinned to the node):

```yaml
apiVersion: v1
kind: Pod
metadata: {name: nsmount-healer, namespace: kube-system}
spec:
  nodeName: <node>
  hostPID: true
  restartPolicy: Never
  containers:
  - name: healer
    image: busybox
    command: ["sh","-c","sleep 3600"]
    securityContext: {privileged: true}
    volumeMounts:
    - {name: kubelet, mountPath: /var/lib/kubelet, mountPropagation: HostToContainer}
  volumes:
  - {name: kubelet, hostPath: {path: /var/lib/kubelet}}
```

Find the container PID with `hostPID` (`grep -l <comm> /proc/*/comm`), the
host source is the driver's `…/globalmount` staging path, the target is the
container's mount path.

Proven 2026-08-18 against freeswitch media-capture: a geesefs OOM left
`/var/run/freeswitch/capture` at `ENOTCONN`; the graft made it readable and
writable in place with zero restart.

The lab (`pkg/driver/lab_reconnect_test.go`, build tag `lab`) runs the real
NodeStage/NodePublish against a bucket, binds the volume into two running
private-namespace "containers" (one read-only), kills the mounter as a plugin
restart does, and has a new driver on the same state file heal and graft:
both read again, the read-write one writes to the bucket, the read-only one
stays read-only. Root, FUSE, geesefs and nsmount-heal needed:

    go test -c -tags lab -o driver.test ./pkg/driver
    LAB_S3_ENDPOINT=… LAB_S3_BUCKET=… LAB_S3_KEY=… LAB_S3_SECRET=… \
      ./driver.test -test.run TestLabReconnect -test.v

## What writers should still do

Even with all three layers, an outage window exists between the mounter dying
and the heal. Durable writers (recordings, faxes, media capture) should pair
this with the spool-and-drain pattern in
[resilient-writers.md](resilient-writers.md): write active files to a local
`emptyDir`, move completed files to the volume (which forces a fresh `open()`
that picks up the healed mount), and never hold an fd open across the outage.
