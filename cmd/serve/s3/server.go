// Package s3 implements an fake s3 server for rclone

package s3

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/rclone/rclone/cmd"
	"github.com/rclone/rclone/cmd/serve/httplib"
	"github.com/rclone/rclone/cmd/serve/httplib/httpflags"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/flags"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/rclone/vfs/vfsflags"
	"github.com/spf13/cobra"
)

var (
	initTime          = time.Now()
	defaultBucketName string
	hostBucketMode    bool
	skipBucketVerify  bool
	hashName          string
	hashType          = hash.None
)

func init() {
	flagSet := Command.Flags()
	httpflags.AddFlags(flagSet)
	vfsflags.AddFlags(flagSet)
	flags.BoolVarP(flagSet, &skipBucketVerify, "skip-bucket-verify", "", false, "Skip bucket verification")
	flags.BoolVarP(flagSet, &hostBucketMode, "host-bucket", "", false, "Whether to use bucket name in hostname (such as mybucket.local)")
	flags.StringVarP(flagSet, &defaultBucketName, "bucket-name", "", "rclone", "Name of the busket to serve")
	flags.StringVarP(flagSet, &hashName, "etag-hash", "", "", "Which hash to use for the ETag, or auto or blank for off")
}

// Command definition for cobra
var Command = &cobra.Command{
	Use:   "s3 remote:path",
	Short: `Serve remote:path over s3.`,
	Long: `
rclone serve s3 implements a basic s3 server to serve the
remote over s3. This can be viewed with s3 client
or you can make a remote of type s3 to read and write it.
Note that some clients may require https endpoint.
 
### S3 options
Use --s3-bucket-name to set the bucket name. By default this is rclone.
` + httplib.Help + vfs.Help,
	RunE: func(command *cobra.Command, args []string) error {
		cmd.CheckArgs(1, 1, command, args)
		f := cmd.NewFsSrc(args)

		hashType = hash.None
		if hashName == "auto" {
			hashType = f.Hashes().GetOne()
		} else if hashName != "" {
			err := hashType.Set(hashName)
			if err != nil {
				return err
			}
		}
		cmd.Run(false, false, command, func() error {
			s := newServer(context.Background(), f, &httpflags.Opt)
			err := s.Serve()
			if err != nil {
				return err
			}
			s.Wait()
			return nil
		})
		return nil
	},
}

// Server is a s3.FileSystem interface
type Server struct {
	*httplib.Server
	f       fs.Fs
	vfs     *vfs.VFS
	faker   *gofakes3.GoFakeS3
	handler http.Handler
	ctx     context.Context // for global config
}

// Make a new S3 Server to serve the remote
func newServer(ctx context.Context, f fs.Fs, opt *httplib.Options) *Server {
	w := &Server{
		f:   f,
		ctx: ctx,
		vfs: vfs.New(f, &vfsflags.Opt),
	}

	var newLogger Logger
	w.faker = gofakes3.New(
		newBackend(w.vfs),
		gofakes3.WithHostBucket(hostBucketMode),
		gofakes3.WithLogger(newLogger),
		gofakes3.WithRequestID(rand.Uint64()),
		gofakes3.WithoutVersioning(),
	)

	w.handler = authMiddleware(w.faker.Server())
	w.Server = httplib.NewServer(w.handler, opt)
	return w
}

func authMiddleware(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, rq *http.Request) {
		// Currently not supported, direct to the handler
		handler.ServeHTTP(w, rq)
	})
}

// func dumpRequestMiddleware(handler http.Handler) http.Handler {
// 	return http.HandlerFunc(func(w http.ResponseWriter, rq *http.Request) {
// 		var formatted, err = httputil.DumpRequest(rq, true)
// 		if err != nil {
// 			fmt.Fprint(w, err)
// 		}

// 		fmt.Printf("%s\n", formatted)
// 		handler.ServeHTTP(w, rq)
// 	})
// }

// logger output formatted message
type Logger struct{}

// print log message
func (l Logger) Print(level gofakes3.LogLevel, v ...interface{}) {
	switch level {
	case gofakes3.LogErr:
		fs.Errorf(nil, fmt.Sprintln(v...))
	case gofakes3.LogWarn:
		fs.Errorf(nil, fmt.Sprintln(v...))
	case gofakes3.LogInfo:
		fs.Infof(nil, fmt.Sprintln(v...))
	default:
		panic("unknown level")
	}
}
