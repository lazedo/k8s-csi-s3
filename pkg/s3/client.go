package s3

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"

	"github.com/golang/glog"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	metadataName = ".metadata.json"
)

type s3Client struct {
	Config *Config
	minio  *minio.Client
	ctx    context.Context
}

// Config holds values to configure the driver
type Config struct {
	AccessKeyID     string
	SecretAccessKey string
	Region          string
	Endpoint        string
	Mounter         string
	Insecure        bool
	// CABundle is a PEM bundle to verify the endpoint's TLS certificate against
	// (private CAs), instead of disabling verification with Insecure.
	CABundle string
	// Bucket, when set, is a COSI-provided EXISTING bucket (from a BucketAccess
	// credentials Secret): volumes mount its root, the driver never creates nor
	// deletes it — that lifecycle belongs to the BucketClaim.
	Bucket string
}

// cosiBucketInfo mirrors the COSI (v1alpha1 / release-0.2) BucketAccess
// credentials Secret payload: key "BucketInfo" holds this JSON document.
type cosiBucketInfo struct {
	Spec struct {
		BucketName string `json:"bucketName"`
		SecretS3   struct {
			Endpoint        string `json:"endpoint"`
			Region          string `json:"region"`
			AccessKeyID     string `json:"accessKeyID"`
			AccessSecretKey string `json:"accessSecretKey"`
		} `json:"secretS3"`
	} `json:"spec"`
}

type FSMeta struct {
	BucketName    string   `json:"Name"`
	Prefix        string   `json:"Prefix"`
	Mounter       string   `json:"Mounter"`
	MountOptions  []string `json:"MountOptions"`
	CapacityBytes int64    `json:"CapacityBytes"`
}

func NewClient(cfg *Config) (*s3Client, error) {
	var client = &s3Client{}

	client.Config = cfg
	u, err := url.Parse(client.Config.Endpoint)
	if err != nil {
		return nil, err
	}
	ssl := u.Scheme == "https"
	endpoint := u.Hostname()
	if u.Port() != "" {
		endpoint = u.Hostname() + ":" + u.Port()
	}

	var transport = &http.Transport{}
	if client.Config.Insecure {
		tlsConfig := &tls.Config{}
		tlsConfig.InsecureSkipVerify = true
		transport.TLSClientConfig = tlsConfig
	} else if client.Config.CABundle != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(client.Config.CABundle)) {
			return nil, fmt.Errorf("caBundle contains no valid PEM certificates")
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: pool}
	}
	minioClient, err := minio.New(endpoint, &minio.Options{
		Transport: transport,
		Creds:     credentials.NewStaticV4(client.Config.AccessKeyID, client.Config.SecretAccessKey, ""),
		Region:    client.Config.Region,
		Secure:    ssl,
	})
	if err != nil {
		return nil, err
	}
	client.minio = minioClient
	client.ctx = context.Background()
	return client, nil
}

func NewClientFromSecret(secret map[string]string) (*s3Client, error) {
	insecure, _ := strconv.ParseBool(secret["insecure"])
	// COSI bridge: a BucketAccess credentials Secret carries everything in the
	// "BucketInfo" JSON document (bucket name + endpoint + keys). caBundle and
	// insecure stay honoured as sidecar keys for private-CA endpoints.
	if info := secret["BucketInfo"]; info != "" {
		var bi cosiBucketInfo
		if err := json.Unmarshal([]byte(info), &bi); err != nil {
			return nil, fmt.Errorf("parsing COSI BucketInfo: %w", err)
		}
		if bi.Spec.BucketName == "" || bi.Spec.SecretS3.Endpoint == "" {
			return nil, fmt.Errorf("COSI BucketInfo missing bucketName or secretS3.endpoint")
		}
		return NewClient(&Config{
			AccessKeyID:     bi.Spec.SecretS3.AccessKeyID,
			SecretAccessKey: bi.Spec.SecretS3.AccessSecretKey,
			Region:          bi.Spec.SecretS3.Region,
			Endpoint:        bi.Spec.SecretS3.Endpoint,
			Mounter:         "",
			Insecure:        insecure,
			CABundle:        secret["caBundle"],
			Bucket:          bi.Spec.BucketName,
		})
	}
	return NewClient(&Config{
		AccessKeyID:     secret["accessKeyID"],
		SecretAccessKey: secret["secretAccessKey"],
		Region:          secret["region"],
		Endpoint:        secret["endpoint"],
		// Mounter is set in the volume preferences, not secrets
		Mounter:  "",
		Insecure: insecure,
		CABundle: secret["caBundle"],
	})
}

func (client *s3Client) BucketExists(bucketName string) (bool, error) {
	return client.minio.BucketExists(client.ctx, bucketName)
}

func (client *s3Client) CreateBucket(bucketName string) error {
	return client.minio.MakeBucket(client.ctx, bucketName, minio.MakeBucketOptions{Region: client.Config.Region})
}

func (client *s3Client) CreatePrefix(bucketName string, prefix string) error {
	if prefix != "" {
		_, err := client.minio.PutObject(client.ctx, bucketName, prefix+"/", bytes.NewReader([]byte("")), 0, minio.PutObjectOptions{})
		if err != nil {
			return err
		}
	}
	return nil
}

func (client *s3Client) RemovePrefix(bucketName string, prefix string) error {
	var err error

	if err = client.removeObjects(bucketName, prefix); err == nil {
		return client.minio.RemoveObject(client.ctx, bucketName, prefix, minio.RemoveObjectOptions{})
	}

	glog.Warningf("removeObjects failed with: %s, will try removeObjectsOneByOne", err)

	if err = client.removeObjectsOneByOne(bucketName, prefix); err == nil {
		return client.minio.RemoveObject(client.ctx, bucketName, prefix, minio.RemoveObjectOptions{})
	}

	return err
}

func (client *s3Client) RemoveBucket(bucketName string) error {
	var err error

	if err = client.removeObjects(bucketName, ""); err == nil {
		return client.minio.RemoveBucket(client.ctx, bucketName)
	}

	glog.Warningf("removeObjects failed with: %s, will try removeObjectsOneByOne", err)

	if err = client.removeObjectsOneByOne(bucketName, ""); err == nil {
		return client.minio.RemoveBucket(client.ctx, bucketName)
	}

	return err
}

func (client *s3Client) removeObjects(bucketName, prefix string) error {
	objectsCh := make(chan minio.ObjectInfo)
	var listErr error

	go func() {
		defer close(objectsCh)

		for object := range client.minio.ListObjects(
			client.ctx,
			bucketName,
			minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
			if object.Err != nil {
				listErr = object.Err
				return
			}
			objectsCh <- object
		}
	}()

	if listErr != nil {
		glog.Error("Error listing objects", listErr)
		return listErr
	}

	select {
	default:
		opts := minio.RemoveObjectsOptions{
			GovernanceBypass: true,
		}
		errorCh := client.minio.RemoveObjects(client.ctx, bucketName, objectsCh, opts)
		haveErrWhenRemoveObjects := false
		for e := range errorCh {
			glog.Errorf("Failed to remove object %s, error: %s", e.ObjectName, e.Err)
			haveErrWhenRemoveObjects = true
		}
		if haveErrWhenRemoveObjects {
			return fmt.Errorf("Failed to remove all objects of bucket %s", bucketName)
		}
	}

	return nil
}

// will delete files one by one without file lock
func (client *s3Client) removeObjectsOneByOne(bucketName, prefix string) error {
	parallelism := 16
	objectsCh := make(chan minio.ObjectInfo, parallelism)
	guardCh := make(chan int, parallelism)
	var listErr error
	var totalObjects int64 = 0
	var removeErrors int64 = 0

	go func() {
		defer close(objectsCh)

		for object := range client.minio.ListObjects(client.ctx, bucketName,
			minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
			if object.Err != nil {
				listErr = object.Err
				return
			}
			atomic.AddInt64(&totalObjects, 1)
			objectsCh <- object
		}
	}()

	if listErr != nil {
		glog.Error("Error listing objects", listErr)
		return listErr
	}

	for object := range objectsCh {
		guardCh <- 1
		go func(obj minio.ObjectInfo) {
			err := client.minio.RemoveObject(client.ctx, bucketName, obj.Key,
				minio.RemoveObjectOptions{VersionID: obj.VersionID})
			if err != nil {
				glog.Errorf("Failed to remove object %s, error: %s", obj.Key, err)
				atomic.AddInt64(&removeErrors, 1)
			}
			<-guardCh
		}(object)
	}
	for i := 0; i < parallelism; i++ {
		guardCh <- 1
	}
	for i := 0; i < parallelism; i++ {
		<-guardCh
	}

	if removeErrors > 0 {
		return fmt.Errorf("Failed to remove %v objects out of total %v of path %s", removeErrors, totalObjects, bucketName)
	}

	return nil
}
