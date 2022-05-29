package s3

import (
	"crypto/md5"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/johannesboyne/gofakes3"
	"github.com/rclone/rclone/vfs"
)

var (
	emptyPrefix = &gofakes3.Prefix{}
	timeFormat  = "Mon, 2 Jan 2006 15:04:05 GMT"
)

type SimpleBucketBackend struct {
	lock sync.Mutex
	fs   *vfs.VFS
}

// newBackend creates a new SimpleBucketBackend.
func newBackend(fs *vfs.VFS) gofakes3.Backend {
	return &SimpleBucketBackend{
		fs: fs,
	}
}

// ListBuckets always returns the default bucket.
func (db *SimpleBucketBackend) ListBuckets() ([]gofakes3.BucketInfo, error) {

	return []gofakes3.BucketInfo{
		{
			Name:         defaultBucketName,
			CreationDate: gofakes3.NewContentTime(initTime),
		},
	}, nil
}

// ListObjects lists the objects in the given bucket.
func (db *SimpleBucketBackend) ListBucket(bucket string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {

	if bucket != defaultBucketName && !skipBucketVerify {
		return nil, gofakes3.BucketNotFound(bucket)
	}
	if prefix == nil {
		prefix = emptyPrefix
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	result, err := db.getObjectsList(prefix)
	if err != nil {
		return nil, err
	}

	return db.pager(result, page)
}

// getObjectsList lists the objects in the given bucket.
func (db *SimpleBucketBackend) getObjectsList(prefix *gofakes3.Prefix) (*gofakes3.ObjectList, error) {

	prefixPath, prefixPart := prefixParser(prefix)
	dirEntries, err := getDirEntries(filepath.FromSlash(prefixPath), db.fs)
	if err != nil {
		return nil, err
	}
	response := gofakes3.NewObjectList()

	for _, entry := range dirEntries {
		object := entry.Name()

		// Expected use of 'path'; see the "Path Handling" subheading in doc.go:
		objectPath := path.Join(prefixPath, object)

		if prefixPart != "" && !strings.HasPrefix(object, prefixPart) {
			continue
		}

		if entry.IsDir() {
			response.AddPrefix(objectPath)

		} else {
			size := entry.Size()
			mtime := entry.ModTime()

			response.Add(&gofakes3.Content{
				Key:          objectPath,
				LastModified: gofakes3.NewContentTime(mtime),
				ETag:         `""`,
				Size:         size,
			})
		}
	}

	return response, nil
}

// HeadObject returns the fileinfo for the given object name.
//
// Note that the metadata is not supported yet.
func (db *SimpleBucketBackend) HeadObject(bucketName, objectName string) (*gofakes3.Object, error) {

	if bucketName != defaultBucketName && !skipBucketVerify {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	stat, err := db.fs.Stat(objectName)
	if err == vfs.ENOENT {
		return nil, gofakes3.KeyNotFound(objectName)
	} else if err != nil {
		return nil, err
	}

	if stat.IsDir() {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	size := stat.Size()
	hash, err := getFileHash(stat)
	if err != nil {
		return nil, err
	}

	return &gofakes3.Object{
		Name: objectName,
		Hash: []byte(hash),
		Metadata: map[string]string{
			"Last-Modified": stat.ModTime().Format(timeFormat),
		},
		Size:     size,
		Contents: NoOpReadCloser{},
	}, nil
}

// GetObject fetchs the object from the filesystem.
func (db *SimpleBucketBackend) GetObject(bucketName, objectName string, rangeRequest *gofakes3.ObjectRangeRequest) (obj *gofakes3.Object, err error) {
	if bucketName != defaultBucketName && !skipBucketVerify {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	f, err := db.fs.Open(objectName)
	if os.IsNotExist(err) {
		return nil, gofakes3.KeyNotFound(objectName)
	} else if err != nil {
		return nil, err
	}

	if f.Node().IsDir() {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	defer func() {
		// If an error occurs, the caller may not have access to Object.Body in order to close it:
		if err != nil && obj == nil {
			f.Close()
		}
	}()

	stat, err := db.fs.Stat(filepath.FromSlash(objectName))
	if err != nil {
		return nil, err
	}

	size := stat.Size()
	hash, err := getFileHash(stat)
	if err != nil {
		return nil, err
	}

	var rdr io.ReadCloser = f
	rnge, err := rangeRequest.Range(size)
	if err != nil {
		return nil, err
	}

	if rnge != nil {
		if _, err := f.Seek(rnge.Start, io.SeekStart); err != nil {
			return nil, err
		}
		rdr = limitReadCloser(rdr, f.Close, rnge.Length)
	}

	return &gofakes3.Object{
		Name: objectName,
		Hash: []byte(hash),
		Metadata: map[string]string{
			"Last-Modified": stat.ModTime().Format(timeFormat),
		},
		Size:     size,
		Range:    rnge,
		Contents: rdr,
	}, nil
}

// PutObject creates or overwrites the object with the given name.
func (db *SimpleBucketBackend) PutObject(
	bucketName, objectName string,
	meta map[string]string,
	input io.Reader, size int64,
) (result gofakes3.PutObjectResult, err error) {

	if bucketName != defaultBucketName && !skipBucketVerify {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	objectDir := filepath.Dir(objectName)

	if objectDir != "." {
		if err := db.fs.Mkdir(objectDir, 0777); err != nil {
			return result, err
		}
	}

	f, err := db.fs.Create(objectName)
	if err != nil {
		return result, err
	}

	hasher := md5.New()
	w := io.MultiWriter(f, hasher)
	if _, err := io.Copy(w, input); err != nil {
		return result, err
	}

	if err := f.Close(); err != nil {
		return result, err
	}

	_, err = db.fs.Stat(objectName)
	if err != nil {
		return result, err
	}

	return result, nil
}

// DeleteMulti deletes multiple objects in a single request.
func (db *SimpleBucketBackend) DeleteMulti(bucketName string, objects ...string) (result gofakes3.MultiDeleteResult, rerr error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	for _, object := range objects {
		if err := db.deleteObjectLocked(bucketName, object); err != nil {
			log.Println("delete object failed:", err)
			result.Error = append(result.Error, gofakes3.ErrorResult{
				Code:    gofakes3.ErrInternal,
				Message: gofakes3.ErrInternal.Message(),
				Key:     object,
			})
		} else {
			result.Deleted = append(result.Deleted, gofakes3.ObjectID{
				Key: object,
			})
		}
	}

	return result, nil
}

// DeleteObject deletes the object with the given name.
func (db *SimpleBucketBackend) DeleteObject(bucketName, objectName string) (result gofakes3.ObjectDeleteResult, rerr error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	return result, db.deleteObjectLocked(bucketName, objectName)
}

// deleteObjectLocked deletes the object from the filesystem.
func (db *SimpleBucketBackend) deleteObjectLocked(bucketName, objectName string) error {
	if bucketName != defaultBucketName && !skipBucketVerify {
		return gofakes3.BucketNotFound(bucketName)
	}

	// S3 does not report an error when attemping to delete a key that does not exist, so
	// we need to skip IsNotExist errors.
	if err := db.fs.Remove(objectName); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

// CreateBucket is unsupported by SimpleBucketBackend.
func (db *SimpleBucketBackend) CreateBucket(name string) error {
	return gofakes3.ErrBucketAlreadyExists
}

// DeleteBucket is unsupported by SimpleBucketBackend.
func (db *SimpleBucketBackend) DeleteBucket(name string) error {
	return gofakes3.ErrBucketNotEmpty
}

// BucketExists checks if the bucket exists.
func (db *SimpleBucketBackend) BucketExists(name string) (exists bool, err error) {
	return true, nil
}
