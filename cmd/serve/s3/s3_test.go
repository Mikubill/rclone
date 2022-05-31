// Serve s3 tests set up a server and run the integration tests
// for the s3 remote against it.

package s3

import (
	"context"
	"testing"

	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/cmd/serve/httplib/httpflags"
	"github.com/rclone/rclone/cmd/serve/servetest"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/hash"
	"github.com/stretchr/testify/assert"
)

const (
	endpoint = "localhost:8080"
)

// TestS3 runs the s3 server then runs the unit tests for the
// s3 remote against it.
func TestS3(t *testing.T) {
	// Configure and start the server
	start := func(f fs.Fs) (configmap.Simple, func()) {
		httpflags.Opt.ListenAddr = endpoint
		serveropt := &Options{
			hostBucketMode: false,
			hashName:       "",
			hashType:       hash.None,
		}

		w := newServer(context.Background(), f, serveropt)
		err := w.Serve()
		assert.NoError(t, err)

		// Config for the backend we'll use to connect to the server
		config := configmap.Simple{
			"type":     "s3",
			"provider": "Other",
			"endpoint": "http://" + endpoint,
		}

		return config, func() {}
	}

	servetest.Run(t, "s3", start)
}
