// nsmount-heal: graft a healthy mount subtree onto a path inside a RUNNING
// container's mount namespace — no restart. C, because setns(CLONE_NEWNS)
// requires a single-threaded caller and the Go runtime is never that.
//
//   nsmount-heal <container-pid> <host-source-path> <container-target-path>
//
// The caller's OWN mount namespace must already see the healthy source
// (the healer pod bind-mounts /var/lib/kubelet with HostToContainer
// propagation, so a freshly-remounted staging tree propagates in). We
// open_tree(CLONE) that source into a DETACHED tree fd — which survives a
// namespace switch — then setns into the container's mnt ns (the only
// setns, done single-threaded here) and move_mount the clone onto the
// dead target. Needs CAP_SYS_ADMIN (privileged + hostPID pod).
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sched.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/mount.h>
#include <linux/mount.h>

#ifndef OPEN_TREE_CLONE
#define OPEN_TREE_CLONE 1
#endif
#ifndef OPEN_TREE_CLOEXEC
#define OPEN_TREE_CLOEXEC O_CLOEXEC
#endif
#ifndef AT_RECURSIVE
#define AT_RECURSIVE 0x8000
#endif
#ifndef MOVE_MOUNT_F_EMPTY_PATH
#define MOVE_MOUNT_F_EMPTY_PATH 0x00000004
#endif

static int sys_open_tree(int dfd, const char *path, unsigned int flags) {
    return syscall(SYS_open_tree, dfd, path, flags);
}
static int sys_move_mount(int from_dfd, const char *from, int to_dfd,
                          const char *to, unsigned int flags) {
    return syscall(SYS_move_mount, from_dfd, from, to_dfd, to, flags);
}

int main(int argc, char **argv) {
    if (argc != 4) {
        fprintf(stderr, "usage: %s <container-pid> <host-source-path> <container-target-path>\n", argv[0]);
        return 2;
    }
    const char *pid = argv[1], *src = argv[2], *target = argv[3];

    // 1) clone the healthy subtree in OUR namespace (sees it via propagation).
    int tree = sys_open_tree(AT_FDCWD, src, OPEN_TREE_CLONE | AT_RECURSIVE | OPEN_TREE_CLOEXEC);
    if (tree < 0) { perror("open_tree(source)"); return 1; }

    // 2) enter the container's mount namespace (single-threaded: OK here).
    char nsp[256];
    snprintf(nsp, sizeof nsp, "/proc/%s/ns/mnt", pid);
    int nsfd = open(nsp, O_RDONLY | O_CLOEXEC);
    if (nsfd < 0) { perror("open container mnt ns"); return 1; }
    if (setns(nsfd, CLONE_NEWNS) < 0) { perror("setns(container mnt)"); return 1; }

    // 3) detach the dead endpoint and graft the clone onto it.
    if (umount2(target, MNT_DETACH) < 0)
        perror("umount2(target) [continuing]");
    if (sys_move_mount(tree, "", AT_FDCWD, target, MOVE_MOUNT_F_EMPTY_PATH) < 0) {
        perror("move_mount(onto target)");
        return 1;
    }
    printf("grafted %s onto %s in mnt ns of pid %s\n", src, target, pid);
    return 0;
}
