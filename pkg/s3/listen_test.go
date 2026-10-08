package s3

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func listenClient(t *testing.T, srv *httptest.Server) *s3Client {
	t.Helper()
	c, err := NewClient(&Config{Endpoint: srv.URL, AccessKeyID: "key", SecretAccessKey: "secret", Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// A signed request for the bucket's created and removed objects under the
// prefix; the pings are skipped, the keys decoded, and the end of the stream
// is an error: what changes next is not heard.
func TestListen(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		switch {
		case r.URL.Path != "/bucket":
			http.Error(w, "path "+r.URL.Path, http.StatusBadRequest)
			return
		case strings.Join(q["events"], ",") != "s3:ObjectCreated:*,s3:ObjectRemoved:*" || q.Get("prefix") != "p/" || q.Get("ping") != "10":
			http.Error(w, "query "+r.URL.RawQuery, http.StatusBadRequest)
			return
		case !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential=key/"):
			http.Error(w, "unsigned", http.StatusForbidden)
			return
		}
		fmt.Fprintln(w, `{"Records":null}`)
		fmt.Fprintln(w, `{"Records":[{"eventName":"s3:ObjectCreated:Put","s3":{"bucket":{"name":"bucket"},"object":{"key":"p%2Fa+b.json"}}},`+
			`{"eventName":"s3:ObjectRemoved:Delete","s3":{"bucket":{"name":"bucket"},"object":{"key":"p%2Fold%2Fc.json"}}}]}`)
	}))
	defer srv.Close()

	connected := 0
	var keys []string
	err := listenClient(t, srv).Listen(context.Background(), "bucket", "p/", func() { connected++ }, func(k string) { keys = append(keys, k) })
	if err == nil {
		t.Fatal("the end of the stream is an error")
	}
	if connected != 1 {
		t.Fatalf("connected %d times, want 1 (err %v)", connected, err)
	}
	if strings.Join(keys, ",") != "p/a b.json,p/old/c.json" {
		t.Fatalf("keys %q", keys)
	}
}

// A refusal (the credentials may not listen, or the endpoint is not MinIO) is
// told apart from the server's trouble.
func TestListenRefused(t *testing.T) {
	for status, refused := range map[int]bool{http.StatusForbidden: true, http.StatusNotImplemented: true, http.StatusServiceUnavailable: false} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "<Error><Code>AccessDenied</Code></Error>", status)
		}))
		connected := false
		err := listenClient(t, srv).Listen(context.Background(), "bucket", "", func() { connected = true }, func(string) {})
		srv.Close()
		var r *ListenRefused
		if connected || err == nil || errors.As(err, &r) != refused {
			t.Errorf("HTTP %d: connected %v, err %v, refused %v", status, connected, err, refused)
		}
	}
}

// A server that never answers -- not even the headers, what MinIO's stream
// did while its writer swallowed the flushes -- is a dead connection too.
func TestListenNoAnswer(t *testing.T) {
	defer func(d time.Duration) { listenIdle = d }(listenIdle)
	listenIdle = 100 * time.Millisecond
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	connected := false
	start := time.Now()
	err := listenClient(t, srv).Listen(context.Background(), "bucket", "", func() { connected = true }, func(string) {})
	if err == nil || connected || time.Since(start) > 5*time.Second {
		t.Fatalf("no answer: err %v, connected %v, after %s", err, connected, time.Since(start))
	}
}

// A stream with no ping for listenIdle is gone; the end of ctx is no error.
func TestListenIdleAndCancel(t *testing.T) {
	defer func(d time.Duration) { listenIdle = d }(listenIdle)
	listenIdle = 100 * time.Millisecond
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()

	start := time.Now()
	err := listenClient(t, srv).Listen(context.Background(), "bucket", "", func() {}, func(string) {})
	if err == nil || time.Since(start) > 5*time.Second {
		t.Fatalf("idle stream: err %v after %s", err, time.Since(start))
	}

	listenIdle = time.Minute
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	if err := listenClient(t, srv).Listen(ctx, "bucket", "", func() {}, func(string) {}); err != nil {
		t.Fatalf("cancelled: %v", err)
	}
}
