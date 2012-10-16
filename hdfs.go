package hdfs

import (
	"errors"
	zyxar "github.com/zyxar/hdfs"
	"io"
	"log"
	"runtime"
)

var (
	filesystem *zyxar.Fs
	ErrInvalid = errors.New("Invalid argument")
)

func init() {
	Connect("", 0, "") // Connect to local filesystem.
}

// Init connects to an HDFS service.  If host is an empty string, it
// connects to the local filesystem.  Indeed, the package is
// initialized to connect to the local filesystem.  If you want to
// connect to a HDFS, recall Init (in main, for example).
func Connect(host string, port uint16, user string) error {
	var err error
	filesystem, err = zyxar.ConnectAsUser(host, port, user)
	if err != nil {
		log.Fatal("Cannot connect to local filesystem")
	}
	return err
}

type File struct {
	file *zyxar.File
	name string
}

func Open(name string) (*File, error) {
	return openFile(name, zyxar.O_RDONLY)
}

func Create(name string) (file *File, err error) {
	return openFile(name, zyxar.O_WRONLY|zyxar.O_CREATE)
}

func openFile(name string, flags int) (*File, error) {
	hdfsFile, e := filesystem.OpenFile(name, flags, 0, 0, 0)
	if hdfsFile == nil || e != nil {
		return nil, e
	}

	runtime.SetFinalizer(hdfsFile, func(file *zyxar.File) {
		filesystem.CloseFile(file)
	})
	f := &File{hdfsFile, name}
	return f, e
}

func (f *File) Read(b []byte) (n int, err error) {
	if f == nil {
		return 0, ErrInvalid
	}
	u, e := filesystem.Read(f.file, b, len(b))
	if u == 0 && len(b) > 0 && e == nil {
		return 0, io.EOF
	}
	return int(u), e
}

func (f *File) Write(b []byte) (n int, err error) {
	if f == nil {
		return 0, ErrInvalid
	}
	u, e := filesystem.Write(f.file, b, len(b))
	return int(u), e
}

func (f *File) Close() error {
	if f.file == nil {
		return ErrInvalid
	}
	e := filesystem.CloseFile(f.file)
	runtime.SetFinalizer(f.file, nil) // Finalizer is no longer needed.
	f.file = nil                      // Prevents from closing it again.
	return e
}
