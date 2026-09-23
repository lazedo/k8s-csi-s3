package mounter

import (
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"time"

	systemd "github.com/coreos/go-systemd/v22/dbus"
	dbus "github.com/godbus/dbus/v5"
	"github.com/golang/glog"

	"github.com/yandex-cloud/k8s-csi-s3/pkg/s3"
)

const (
	geesefsCmd    = "geesefs"
)

// Implements Mounter
type geesefsMounter struct {
	meta            *s3.FSMeta
	endpoint        string
	region          string
	accessKeyID     string
	secretAccessKey string
	// caBundle (PEM) verifies a private-CA endpoint. geesefs is aws-sdk based,
	// so it is delivered via AWS_CA_BUNDLE pointing at a file written into the
	// plugin dir (shared host<->container, like the copied geesefs binary).
	caBundle     string
	caBundleCtr  string // container-side path of the written bundle
	caBundleHost string // host-side path (systemd unit environment)
}

func newGeeseFSMounter(meta *s3.FSMeta, cfg *s3.Config) (Mounter, error) {
	return &geesefsMounter{
		meta:            meta,
		endpoint:        cfg.Endpoint,
		region:          cfg.Region,
		accessKeyID:     cfg.AccessKeyID,
		secretAccessKey: cfg.SecretAccessKey,
		caBundle:        cfg.CABundle,
	}, nil
}

// writeCABundle materialises the PEM bundle into the plugin dir (visible on the
// host for systemd-run geesefs and at /csi inside the container) and appends it
// to the container's system trust store: geesefs builds its own http.Client over
// a transport with a nil TLSClientConfig, which ignores AWS_CA_BUNDLE but reads
// the system roots — and every mount is a fresh process, so the updated bundle
// is picked up. The env vars are still exported for SDK paths that honour them.
func (geesefs *geesefsMounter) writeCABundle() error {
	if geesefs.caBundle == "" {
		return nil
	}
	sum := sha256.Sum256([]byte(geesefs.caBundle))
	name := fmt.Sprintf("ca-bundle-%x.pem", sum[:4])
	pluginDir := os.Getenv("PLUGIN_DIR")
	if pluginDir == "" {
		pluginDir = "/var/lib/kubelet/plugins/csi.lazedo.dev"
	}
	geesefs.caBundleCtr = "/csi/" + name
	geesefs.caBundleHost = pluginDir + "/" + name
	if err := os.WriteFile(geesefs.caBundleCtr, []byte(geesefs.caBundle), 0644); err != nil {
		return err
	}
	return TrustCABundle(geesefs.caBundle)
}

func (geesefs *geesefsMounter) CopyBinary(from, to string) error {
	st, err := os.Stat(from)
	if err != nil {
		return fmt.Errorf("Failed to stat %s: %v", from, err)
	}
	st2, err := os.Stat(to)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("Failed to stat %s: %v", to, err)
	}
	if err != nil || st2.Size() != st.Size() || st2.ModTime() != st.ModTime() {
		if err == nil {
			// remove the file first to not hit "text file busy" errors
			err = os.Remove(to)
			if err != nil {
				return fmt.Errorf("Error removing %s to update it: %v", to, err)
			}
		}
		bin, err := os.ReadFile(from)
		if err != nil {
			return fmt.Errorf("Error copying %s to %s: %v", from, to, err)
		}
		err = os.WriteFile(to, bin, 0755)
		if err != nil {
			return fmt.Errorf("Error copying %s to %s: %v", from, to, err)
		}
		err = os.Chtimes(to, st.ModTime(), st.ModTime())
		if err != nil {
			return fmt.Errorf("Error copying %s to %s: %v", from, to, err)
		}
	}
	return nil
}

func (geesefs *geesefsMounter) MountDirect(target string, args []string) error {
	args = append([]string{
		"--endpoint", geesefs.endpoint,
		"-o", "allow_other",
		"--log-file", "/dev/stderr",
	}, args...)
	envs := []string{
		"AWS_ACCESS_KEY_ID=" + geesefs.accessKeyID,
		"AWS_SECRET_ACCESS_KEY=" + geesefs.secretAccessKey,
	}
	if geesefs.caBundleCtr != "" {
		envs = append(envs, "AWS_CA_BUNDLE="+geesefs.caBundleCtr)
	}
	return fuseMount(target, geesefsCmd, args, envs)
}

type execCmd struct {
	Path             string
	Args             []string
	UncleanIsFailure bool
}

// unitEnvironment is the environment for the systemd-run geesefs (host side):
// credentials plus, when set, the CA bundle by its host path.
func (geesefs *geesefsMounter) unitEnvironment() []string {
	envs := []string{
		"AWS_ACCESS_KEY_ID=" + geesefs.accessKeyID,
		"AWS_SECRET_ACCESS_KEY=" + geesefs.secretAccessKey,
	}
	if geesefs.caBundleHost != "" {
		envs = append(envs, "AWS_CA_BUNDLE="+geesefs.caBundleHost)
	}
	return envs
}

func (geesefs *geesefsMounter) Mount(target, volumeID string) error {
	if err := geesefs.writeCABundle(); err != nil {
		return fmt.Errorf("writing CA bundle: %v", err)
	}
	fullPath := fmt.Sprintf("%s:%s", geesefs.meta.BucketName, geesefs.meta.Prefix)
	var args []string
	if geesefs.region != "" {
		args = append(args, "--region", geesefs.region)
	}
	args = append(
		args,
		"--setuid", "65534", // nobody. drop root privileges
		"--setgid", "65534", // nogroup
	)
	var unsafeArgs []string
	useSystemd := true
	for i := 0; i < len(geesefs.meta.MountOptions); i++ {
		opt := geesefs.meta.MountOptions[i]
		if opt == "--no-systemd" {
			useSystemd = false
		} else if opt == WarmUpOption {
			continue // the driver's, not geesefs's
		} else if len(opt) > 0 && opt[0] == '-' {
			// Remove unsafe options
			s := 1
			if len(opt) > 1 && opt[1] == '-' {
				s++
			}
			key := opt[s:]
			e := strings.Index(opt, "=")
			if e >= 0 {
				key = opt[s:e]
			}
			if key == "log-file" || key == "shared-config" || key == "cache" {
				// Skip options accessing local FS
				unsafeArgs = append(unsafeArgs, opt)
				i++
				if i < len(geesefs.meta.MountOptions) {
					unsafeArgs = append(unsafeArgs, geesefs.meta.MountOptions[i])
				}
			} else if key != "" {
				args = append(args, opt)
			}
		} else if len(opt) > 0 {
			args = append(args, opt)
		}
	}
	if !useSystemd {
		// Unsafe options are allowed when running inside the container
		args = append(args, unsafeArgs...)
	}
	args = append(args, fullPath, target)
	// Try to start geesefs using systemd so it doesn't get killed when the container exits
	if !useSystemd {
		return geesefs.MountDirect(target, args)
	}
	conn, err := systemd.New()
	if err != nil {
		glog.Errorf("Failed to connect to systemd dbus service: %v, starting geesefs directly", err)
		return geesefs.MountDirect(target, args)
	}
	defer conn.Close()
	// systemd is present
	if err = geesefs.CopyBinary("/usr/bin/geesefs", "/csi/geesefs"); err != nil {
		return err
	}
	pluginDir := os.Getenv("PLUGIN_DIR")
	if pluginDir == "" {
		pluginDir = "/var/lib/kubelet/plugins/csi.lazedo.dev"
	}
	args = append([]string{pluginDir+"/geesefs", "-f", "-o", "allow_other", "--endpoint", geesefs.endpoint}, args...)
	glog.Info("Starting geesefs using systemd: "+strings.Join(args, " "))
	unitName := "geesefs-"+systemd.PathBusEscape(volumeID)+".service"
	newProps := []systemd.Property{
		systemd.Property{
			Name: "Description",
			Value: dbus.MakeVariant("GeeseFS mount for Kubernetes volume "+volumeID),
		},
		systemd.PropExecStart(args, false),
		systemd.Property{
			Name: "Environment",
			Value: dbus.MakeVariant(geesefs.unitEnvironment()),
		},
		systemd.Property{
			Name: "CollectMode",
			Value: dbus.MakeVariant("inactive-or-failed"),
		},
	}
	unitProps, err := conn.GetAllProperties(unitName)
	if err == nil {
		// Unit already exists
		if s, ok := unitProps["ActiveState"].(string); ok && (s == "active" || s == "activating" || s == "reloading") {
			// Unit is already active
			curPath := ""
			prevExec, ok := unitProps["ExecStart"].([][]interface{})
			if ok && len(prevExec) > 0 && len(prevExec[0]) >= 2 {
				execArgs, ok := prevExec[0][1].([]string)
				if ok && len(execArgs) >= 2 {
					curPath = execArgs[len(execArgs)-1]
				}
			}
			if curPath != target {
				// FIXME This may mean that the same bucket&path are used for multiple PVs. Support it somehow
				return fmt.Errorf(
					"GeeseFS for volume %v is already mounted on host, but"+
					" in a different directory. We want %v, but it's in %v",
					volumeID, target, curPath,
				)
			}
			// Already mounted at right location, wait for mount
			return waitForMount(target, 30*time.Second)
		} else {
			// Stop and garbage collect the unit if automatic collection didn't work for some reason
			conn.StopUnit(unitName, "replace", nil)
			conn.ResetFailedUnit(unitName)
		}
	}
	unitPath := "/run/systemd/system/" + unitName + ".d"
	err = os.MkdirAll(unitPath, 0755)
	if err != nil {
		return fmt.Errorf("Error creating directory %s: %v", unitPath, err)
	}
	// force & lazy unmount to cleanup possibly dead mountpoints
	err = os.WriteFile(
		unitPath+"/50-StopProps.conf",
		[]byte("[Service]\nExecStopPost=/bin/umount -f -l "+target+"\nTimeoutStopSec=20\n"),
		0600,
	)
	if err != nil {
		return fmt.Errorf("Error writing %v/50-ExecStopPost.conf: %v", unitPath, err)
	}
	_, err = conn.StartTransientUnit(unitName, "replace", newProps, nil)
	if err != nil {
		return fmt.Errorf("Error starting systemd unit %s on host: %v", unitName, err)
	}
	return waitForMount(target, 30*time.Second)
}
