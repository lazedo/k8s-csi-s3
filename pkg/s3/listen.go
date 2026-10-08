package s3

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7/pkg/notification"
	"github.com/minio/minio-go/v7/pkg/s3utils"
	"github.com/minio/minio-go/v7/pkg/signer"
)

const (
	// listenPing is how often the server sends an empty record on a quiet
	// stream.
	listenPing = 10 * time.Second
	// emptySHA256 is the payload hash of a request without a body.
	emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

// listenIdle without a record means the connection is gone.
var listenIdle = 3 * listenPing

// ListenRefused is a listen request the server answered with an error: the
// endpoint is not MinIO, or the credentials may not listen to the bucket.
// Trying again does not help.
type ListenRefused struct {
	Status int
	Body   string
}

func (e *ListenRefused) Error() string {
	return fmt.Sprintf("listen refused: HTTP %d: %s", e.Status, e.Body)
}

// Listen follows the objects created and removed in bucket under prefix —
// MinIO's ListenBucketNotification — until ctx ends or the stream breaks.
// connected runs once the server has taken the request, before any change is
// reported; changed gets the key of each object, decoded. The error says why
// the stream ended; it is nil when ctx did.
//
// minio-go has the same call, but it reconnects by itself without saying so,
// and whoever keeps a cache needs to know: what changed while nobody was
// listening was never heard.
func (client *s3Client) Listen(ctx context.Context, bucket, prefix string, connected func(), changed func(key string)) error {
	u, err := url.Parse(client.Config.Endpoint)
	if err != nil {
		return err
	}
	u.Path = "/" + bucket
	q := url.Values{}
	q.Set("ping", strconv.Itoa(int(listenPing/time.Second)))
	q.Set("prefix", prefix)
	q.Set("suffix", "")
	q["events"] = []string{"s3:ObjectCreated:*", "s3:ObjectRemoved:*"}
	u.RawQuery = s3utils.QueryEncode(q)

	// The server answers with its first record, a ping at the latest: the
	// watchdog runs from the request on, so a server that never answers is a
	// dead connection too.
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	alive := make(chan struct{}, 1)
	go func() {
		t := time.NewTimer(listenIdle)
		defer t.Stop()
		for {
			select {
			case <-connCtx.Done():
				return
			case <-alive:
				t.Reset(listenIdle)
			case <-t.C:
				cancel()
				return
			}
		}
	}()

	req, err := http.NewRequestWithContext(connCtx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Amz-Content-Sha256", emptySHA256)
	region := client.Config.Region
	if region == "" {
		region = "us-east-1"
	}
	req = signer.SignV4(*req, client.Config.AccessKeyID, client.Config.SecretAccessKey, "", region)

	resp, err := (&http.Client{Transport: client.transport}).Do(req)
	if err != nil {
		switch {
		case ctx.Err() != nil:
			return nil
		case connCtx.Err() != nil:
			return fmt.Errorf("listen: no answer from the server in %s", listenIdle)
		}
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		text := strings.TrimSpace(string(body))
		if resp.StatusCode >= 500 && resp.StatusCode != http.StatusNotImplemented {
			return fmt.Errorf("listen: HTTP %d: %s", resp.StatusCode, text) // the server's trouble, not a refusal
		}
		return &ListenRefused{Status: resp.StatusCode, Body: text}
	}
	select {
	case alive <- struct{}{}:
	default:
	}
	connected()

	dec := json.NewDecoder(resp.Body)
	for {
		var info notification.Info
		if err := dec.Decode(&info); err != nil {
			switch {
			case ctx.Err() != nil:
				return nil
			case connCtx.Err() != nil:
				return fmt.Errorf("listen: nothing from the server in %s", listenIdle)
			}
			return fmt.Errorf("listen: %w", err)
		}
		select {
		case alive <- struct{}{}:
		default:
		}
		for _, r := range info.Records {
			key, err := url.QueryUnescape(r.S3.Object.Key)
			if err != nil {
				key = r.S3.Object.Key
			}
			if key != "" {
				changed(key)
			}
		}
	}
}
