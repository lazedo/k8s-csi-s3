/*
COSI handle mode: instead of a nodeStageSecretRef, the volume names a
BucketAccess (via volumeContext). The driver resolves it in-cluster, GATES on
BucketClaim.status.bucketReady and BucketAccess.status.accessGranted (retrying
until both are true), then reads the BucketAccess credentials Secret. This makes
"create BucketClaim+BucketAccess+PVC together" race-free.
*/
package driver

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// volumeContext keys carrying the COSI handle (stamped by CreateVolume).
const (
	bucketAccessNameKey = "csi.lazedo.dev/bucket-access-name"
	bucketAccessNsKey   = "csi.lazedo.dev/bucket-access-namespace"
	// PVC annotation naming the BucketAccess, and the external-provisioner
	// extra-create-metadata keys carrying the PVC identity into CreateVolume.
	bucketAccessAnnotation = "csi.lazedo.dev/bucket-access"
	pvcNameParam           = "csi.storage.k8s.io/pvc/name"
	pvcNamespaceParam      = "csi.storage.k8s.io/pvc/namespace"
)

var (
	gvrBucketAccess = schema.GroupVersionResource{Group: "objectstorage.k8s.io", Version: "v1alpha1", Resource: "bucketaccesses"}
	gvrBucketClaim  = schema.GroupVersionResource{Group: "objectstorage.k8s.io", Version: "v1alpha1", Resource: "bucketclaims"}
)

// k8sClients holds the in-cluster clients the node server uses to resolve COSI
// handles. nil when out of cluster (COSI handle mode then unavailable).
type k8sClients struct {
	typed   kubernetes.Interface
	dynamic dynamic.Interface
}

func newK8sClients() (*k8sClients, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	typed, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &k8sClients{typed: typed, dynamic: dyn}, nil
}

// resolveCOSISecret resolves a BucketAccess handle to the secret map that
// NewClientFromSecret expects (the BucketInfo JSON). It returns a non-nil error
// while the bucket/access are not yet ready — the CSI caller retries, which is
// the intended wait.
func (c *k8sClients) resolveCOSISecret(ctx context.Context, name, namespace string) (map[string]string, error) {
	if c == nil {
		return nil, fmt.Errorf("no in-cluster client to resolve BucketAccess %s/%s", namespace, name)
	}
	ba, err := c.dynamic.Resource(gvrBucketAccess).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get BucketAccess %s/%s: %w", namespace, name, err)
	}
	granted, _, _ := unstructured.NestedBool(ba.Object, "status", "accessGranted")
	claimName, _, _ := unstructured.NestedString(ba.Object, "spec", "bucketClaimName")
	secretName, _, _ := unstructured.NestedString(ba.Object, "spec", "credentialsSecretName")
	if claimName == "" || secretName == "" {
		return nil, fmt.Errorf("BucketAccess %s/%s missing bucketClaimName or credentialsSecretName", namespace, name)
	}

	claim, err := c.dynamic.Resource(gvrBucketClaim).Namespace(namespace).Get(ctx, claimName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get BucketClaim %s/%s: %w", namespace, claimName, err)
	}
	ready, _, _ := unstructured.NestedBool(claim.Object, "status", "bucketReady")

	// gate: wait (via retryable error) until both are true
	if !ready {
		return nil, fmt.Errorf("BucketClaim %s/%s not ready yet", namespace, claimName)
	}
	if !granted {
		return nil, fmt.Errorf("BucketAccess %s/%s access not granted yet", namespace, name)
	}

	sec, err := c.typed.CoreV1().Secrets(namespace).Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get credentials Secret %s/%s: %w", namespace, secretName, err)
	}
	return secretDataToStringMap(sec), nil
}

func secretDataToStringMap(sec *corev1.Secret) map[string]string {
	out := map[string]string{}
	for k, v := range sec.Data {
		out[k] = string(v)
	}
	return out
}

// bucketAccessHandle reads the PVC named by the extra-create-metadata params and
// returns the BucketAccess name+namespace from its csi.lazedo.dev/bucket-access
// annotation. ok=false when there's no PVC handle (non-COSI volume).
func (d *driver) bucketAccessHandle(ctx context.Context, params map[string]string) (name, namespace string, ok bool) {
	pvcName, pvcNS := params[pvcNameParam], params[pvcNamespaceParam]
	if pvcName == "" || pvcNS == "" || d.k8s == nil {
		return "", "", false
	}
	pvc, err := d.k8s.typed.CoreV1().PersistentVolumeClaims(pvcNS).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		return "", "", false
	}
	ba := pvc.Annotations[bucketAccessAnnotation]
	if ba == "" {
		return "", "", false
	}
	return ba, pvcNS, true
}

// secretsForVolume returns the secret map to build the S3 client with: the
// kubelet-provided nodeStageSecretRef normally, or — when the volume carries a
// COSI BucketAccess handle in its context — the resolved (and gated) credentials.
func (d *driver) secretsForVolume(ctx context.Context, csiSecrets, volumeContext map[string]string) (map[string]string, error) {
	name := volumeContext[bucketAccessNameKey]
	if name == "" {
		return csiSecrets, nil
	}
	ns := volumeContext[bucketAccessNsKey]
	if ns == "" {
		return nil, fmt.Errorf("%s set without %s", bucketAccessNameKey, bucketAccessNsKey)
	}
	return d.k8s.resolveCOSISecret(ctx, name, ns)
}
