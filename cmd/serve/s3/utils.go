package s3

import (
	"context"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/vfs"
)

type NoOpReadCloser struct{}

type ReaderWithDummyCloser struct {
	io.Reader
}

type readerWithCloser struct {
	io.Reader
	closer func() error
}

var _ io.ReadCloser = &readerWithCloser{}

func (d ReaderWithDummyCloser) Close() error {
	return nil
}

func (d NoOpReadCloser) Read(b []byte) (n int, err error) {
	return 0, io.EOF
}

func (d NoOpReadCloser) Close() error {
	return nil
}

func limitReadCloser(rdr io.Reader, closer func() error, sz int64) io.ReadCloser {
	return &readerWithCloser{
		Reader: io.LimitReader(rdr, sz),
		closer: closer,
	}
}

func (rwc *readerWithCloser) Close() error {
	if rwc.closer != nil {
		return rwc.closer()
	}
	return nil
}

func getDirEntries(prefix string, fs *vfs.VFS) (vfs.Nodes, error) {
	node, err := fs.Stat(prefix)

	if err == vfs.ENOENT {
		return nil, gofakes3.ErrNoSuchKey
	} else if err != nil {
		return nil, err
	}

	if !node.IsDir() {
		return nil, gofakes3.ErrNoSuchKey
	}

	dir := node.(*vfs.Dir)
	dirEntries, err := dir.ReadDirAll()
	if err != nil {
		return nil, err
	}

	return dirEntries, nil
}

func getFileHash(node vfs.Node) string {
	if node.IsDir() {
		return ""
	}

	o, ok := node.DirEntry().(fs.Object)
	if !ok {
		return ""
	}

	hash, _ := o.Hash(context.Background(), Opt.hashType)
	return hash
}

func parseTimestamp(ts string) (time.Time, error) {
	v := strings.Split(ts, ".")
	if len(v[1]) > 0 {
		for len(v[1]) < 9 {
			v[1] += "0"
		}
	}
	a, err := strconv.ParseInt(v[0], 10, 64)
	if err != nil {
		return time.Time{}, err
	}

	b, err := strconv.ParseInt(v[1], 10, 64)
	if err != nil {
		return time.Time{}, err
	}

	return time.Unix(a, b), nil
}

func prefixParser(p *gofakes3.Prefix) (path, remaining string, ok bool) {
	if !p.HasDelimiter || p.Delimiter != "/" {
		return "", "", ok
	}

	idx := strings.LastIndexByte(p.Prefix, '/')
	if idx < 0 {
		return "", p.Prefix, true
	} else {
		return p.Prefix[:idx], p.Prefix[idx+1:], true
	}
}

func mkdirRecursive(path string, fs *vfs.VFS) error {
	dirs := strings.Split(path, "/")
	dir := ""
	for _, d := range dirs {
		dir += "/" + d
		if _, err := fs.Stat(dir); err != nil {
			err := fs.Mkdir(dir, 0777)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
