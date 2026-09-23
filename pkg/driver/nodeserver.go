/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"syscall"

	"github.com/golang/glog"
	"github.com/yandex-cloud/k8s-csi-s3/pkg/mounter"
	"github.com/yandex-cloud/k8s-csi-s3/pkg/s3"
	mount "k8s.io/mount-utils"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type nodeServer struct {
	csi.UnimplementedNodeServer
	driver *driver
}

func getMeta(bucketName, prefix string, context map[string]string) *s3.FSMeta {
	// COSI bridge: the controller stamped the BucketAccess bucket in the volume
	// context — mount its root, regardless of what the volume ID encodes.
	if b := context[cosiBucketContextKey]; b != "" {
		bucketName = b
		prefix = ""
	}
	mountOptions := make([]string, 0)
	mountOptStr := context[mounter.OptionsKey]
	if mountOptStr != "" {
		re, _ := regexp.Compile(`([^\s"]+|"([^"\\]+|\\")*")+`)
		re2, _ := regexp.Compile(`"([^"\\]+|\\")*"`)
		re3, _ := regexp.Compile(`\\(.)`)
		for _, opt := range re.FindAll([]byte(mountOptStr), -1) {
			// Unquote options
			opt = re2.ReplaceAllFunc(opt, func(q []byte) []byte {
				return re3.ReplaceAll(q[1:len(q)-1], []byte("$1"))
			})
			mountOptions = append(mountOptions, string(opt))
		}
	}
	capacity, _ := strconv.ParseInt(context["capacity"], 10, 64)
	return &s3.FSMeta{
		BucketName:    bucketName,
		Prefix:        prefix,
		Mounter:       context[mounter.TypeKey],
		MountOptions:  mountOptions,
		CapacityBytes: capacity,
	}
}

func (ns *nodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	targetPath := req.GetTargetPath()
	stagingTargetPath := req.GetStagingTargetPath()

	// Check arguments
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability missing in request")
	}
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if len(stagingTargetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Staging Target path missing in request")
	}
	if len(targetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}

	notMnt, err := checkMount(stagingTargetPath)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if notMnt {
		// Staged mount is dead by some reason. Revive it
		if err := ns.mountStaged(ctx, volumeID, stagingTargetPath, req.VolumeContext, req.GetSecrets()); err != nil {
			return nil, err
		}
		ns.driver.sup.recordStage(volumeID, stagingTargetPath, req.VolumeContext, req.GetSecrets())
	}

	notMnt, err = checkMount(targetPath)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !notMnt {
		return &csi.NodePublishVolumeResponse{}, nil
	}

	// TODO: Implement readOnly & mountFlags
	readOnly := req.GetReadonly()
	mountFlags := req.GetVolumeCapability().GetMount().GetMountFlags()
	attrib := req.GetVolumeContext()

	glog.V(4).Infof("target %v\nreadonly %v\nvolumeId %v\nattributes %v\nmountflags %v\n",
		targetPath, readOnly, volumeID, attrib, mountFlags)

	cmd := exec.Command("mount", "--bind", stagingTargetPath, targetPath)
	cmd.Stderr = os.Stderr
	glog.V(3).Infof("Binding volume %v from %v to %v", volumeID, stagingTargetPath, targetPath)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("Error running mount --bind %v %v: %s", stagingTargetPath, targetPath, out)
	}

	glog.V(4).Infof("s3: volume %s successfully mounted to %s", volumeID, targetPath)
	ns.driver.sup.recordPublish(volumeID, targetPath)

	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	targetPath := req.GetTargetPath()

	// Check arguments
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if len(targetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}

	if err := mounter.Unmount(targetPath); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	ns.driver.sup.forgetPublish(volumeID, targetPath)
	glog.V(4).Infof("s3: volume %s has been unmounted.", volumeID)

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	stagingTargetPath := req.GetStagingTargetPath()

	// Check arguments
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	if len(stagingTargetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}

	if req.VolumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "NodeStageVolume Volume Capability must be provided")
	}

	notMnt, err := checkMount(stagingTargetPath)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !notMnt {
		// already mounted — still (re)record for the supervisor (plugin restarts
		// lose in-memory state; the state file survives, this is belt+braces).
		ns.driver.sup.recordStage(volumeID, stagingTargetPath, req.VolumeContext, req.GetSecrets())
		return &csi.NodeStageVolumeResponse{}, nil
	}
	if err := ns.mountStaged(ctx, volumeID, stagingTargetPath, req.VolumeContext, req.GetSecrets()); err != nil {
		return nil, err
	}
	ns.driver.sup.recordStage(volumeID, stagingTargetPath, req.VolumeContext, req.GetSecrets())
	ns.driver.sup.warmIfOptedIn(volumeID)
	return &csi.NodeStageVolumeResponse{}, nil
}

// mountStaged mounts a volume at its staging path from its context+secrets —
// shared by NodeStageVolume, the NodePublish revive path and the supervisor.
func (ns *nodeServer) mountStaged(ctx context.Context, volumeID, stagingTargetPath string, volumeContext, csiSecrets map[string]string) error {
	bucketName, prefix := volumeIDToBucketPrefix(volumeID)
	secrets, err := ns.driver.secretsForVolume(ctx, csiSecrets, volumeContext)
	if err != nil {
		return err
	}
	client, err := s3.NewClientFromSecret(secrets)
	if err != nil {
		return fmt.Errorf("failed to initialize S3 client: %s", err)
	}
	// COSI bridge: mount the BucketAccess-named bucket's root.
	if client.Config.Bucket != "" {
		bucketName, prefix = client.Config.Bucket, ""
	}
	meta := getMeta(bucketName, prefix, volumeContext)
	m, err := mounter.New(meta, client.Config)
	if err != nil {
		return err
	}
	return m.Mount(stagingTargetPath, volumeID)
}

func (ns *nodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	stagingTargetPath := req.GetStagingTargetPath()

	// Check arguments
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if len(stagingTargetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}

	proc, err := mounter.FindFuseMountProcess(stagingTargetPath)
	if err != nil {
		return nil, err
	}
	exists := false
	if proc == nil {
		exists, err = mounter.SystemdUnmount(volumeID)
		if exists && err != nil {
			return nil, err
		}
	}
	if !exists {
		err = mounter.FuseUnmount(stagingTargetPath)
	}
	ns.driver.sup.forgetStage(volumeID)
	glog.V(4).Infof("s3: volume %s has been unmounted from stage path %v.", volumeID, stagingTargetPath)

	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodeGetCapabilities returns the supported capabilities of the node server
func (ns *nodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	nscap := &csi.NodeServiceCapability{
		Type: &csi.NodeServiceCapability_Rpc{
			Rpc: &csi.NodeServiceCapability_RPC{
				Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
			},
		},
	}

	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			nscap,
		},
	}, nil
}

func (ns *nodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	return &csi.NodeExpandVolumeResponse{}, status.Error(codes.Unimplemented, "NodeExpandVolume is not implemented")
}

func (ns *nodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: ns.driver.nodeID,
	}, nil
}

func (ns *nodeServer) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "NodeGetVolumeStats is not implemented")
}

func checkMount(targetPath string) (bool, error) {
	notMnt, err := mount.New("").IsLikelyNotMountPoint(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err = os.MkdirAll(targetPath, 0750); err != nil {
				return false, err
			}
			notMnt = true
		} else if errors.Is(err, syscall.ENOTCONN) || errors.Is(err, syscall.EIO) {
			// dead FUSE endpoint (the mounter died): detach the corpse and
			// report "not mounted" so the caller remounts.
			glog.Warningf("dead mount endpoint at %s — detaching", targetPath)
			lazyUnmount(targetPath)
			notMnt = true
		} else {
			return false, err
		}
	}
	return notMnt, nil
}
