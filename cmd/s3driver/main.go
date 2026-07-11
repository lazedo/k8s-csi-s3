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

package main

import (
	"flag"
	"log"
	"os"

	"github.com/yandex-cloud/k8s-csi-s3/pkg/driver"
	"github.com/yandex-cloud/k8s-csi-s3/pkg/mounter"
)

func init() {
	flag.Set("logtostderr", "true")
}

var (
	endpoint = flag.String("endpoint", "unix://tmp/csi.sock", "CSI endpoint")
	nodeID   = flag.String("nodeid", "", "node id")
)

func main() {
	flag.Parse()

	// Trust a cluster/private CA cluster-wide when mounted (CA_BUNDLE_FILE): the
	// COSI BucketAccess secret carries no caBundle, so private-CA endpoints are
	// verified via the container trust store instead of per-secret.
	if f := os.Getenv("CA_BUNDLE_FILE"); f != "" {
		if pem, err := os.ReadFile(f); err == nil {
			if err := mounter.TrustCABundle(string(pem)); err != nil {
				log.Printf("warning: trusting CA_BUNDLE_FILE %s: %v", f, err)
			}
		} else {
			log.Printf("warning: reading CA_BUNDLE_FILE %s: %v", f, err)
		}
	}

	driver, err := driver.New(*nodeID, *endpoint)
	if err != nil {
		log.Fatal(err)
	}
	driver.Run()
	os.Exit(0)
}
