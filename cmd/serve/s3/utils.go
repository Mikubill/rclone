package s3

import (
	"context"
	"errors"
	"io"
	"strings"

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

func getFileHash(node vfs.Node) (string, error) {
	if node.IsDir() {
		return "", errors.New("not a file")
	}

	o, ok := node.DirEntry().(fs.Object)
	if !ok {
		return "", errors.New("not a file")
	}

	return o.Hash(context.Background(), Opt.hashType)
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
