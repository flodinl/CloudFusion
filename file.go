// adapted from https://blog.gopheracademy.com/advent-2014/fuse-zipfs/ tutorial

package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
	"os"
	"time"
)

/*
Struct representing a file in the FUSE file system.
*/
type File struct {
	inode       *Inode
	inodeNum    uint64
	inodeStream *IntStream
}

var _ fs.Node = (*File)(nil)

/*
FUSE method that returns metadata about a particular file.
*/
func (f *File) Attr(ctx context.Context, attr *fuse.Attr) error {
	// fmt.Printf("getting attr of file with inode %d\n", f.inodeNum)
	attr.Size = f.inode.Size
	var fileMode os.FileMode = 0
	if f.inode.IsDir == 1 {
		fileMode = 1 << 31
	}
	attr.Mode = fileMode
	fileTime := time.Unix(f.inode.UnixTime, 0)
	attr.Mtime = fileTime
	attr.Ctime = fileTime
	attr.Crtime = fileTime
	return nil
}

// var _ = fs.NodeSetattrer(&File{})

// /*
// FUSE method that updates the metadata of a particular file. Importantly, this updates the size,
// which is necessary for reading/writing correctly.

// This never seems to actually be called, so file size is set manually elsewhere.
// */
// func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
// 	// fmt.Printf("doing setattr of file with inode %d\n", f.inodeNum)
// 	// should other things be set as well?
// 	if req.Valid.Size() {
// 		// fmt.Printf("set size of file in setAttr")
// 		f.inode.Size = req.Size
// 	}
// 	// this is sort of inaccurate but probably good enough
// 	if req.Valid.Mtime() || req.Valid.Atime() {
// 		f.inode.UnixTime = req.Mtime.Unix()
// 	}
// 	err := putInode(f.inode, f.inodeNum)
// 	return err
// }

var _ = fs.NodeOpener(&File{})

/*
FUSE method that returns a file handle for a file in the file system.
*/
func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	// fmt.Printf("opening file with inodeNum: %d\n", f.inodeNum)
	handle := &FileHandle{
		inode:    f.inode,
		inodeNum: f.inodeNum,
	}
	return handle, nil
}

/*
Struct that represents a file handle for a File struct.
*/
type FileHandle struct {
	inode    *Inode
	inodeNum uint64
}

var _ fs.Handle = (*FileHandle)(nil)

var _ fs.HandleReleaser = (*FileHandle)(nil)

/*
FUSE method that closes a file handle associated with a file, causing the file to be uploaded.
*/
func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	err := putInode(fh.inode, fh.inodeNum)
	return err
}

var _ = fs.HandleReader(&FileHandle{})

/*
FUSE method that reads from a file handle with a particular offset and size, and puts the result
into the response.
*/
func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	// fmt.Printf("reading from file with inodeNum: %d\n", fh.inodeNum)
	// fmt.Printf("in file read inode size is: %d, req size is: %d\n", fh.inode.Size, req.Size)
	size := uint64(req.Size)
	// if size > fh.inode.Size {
	// 	return fuse.ESTALE
	// }
	data, err := fh.inode.readFromData(uint64(req.Offset), size)
	resp.Data = data
	return err
}

var _ = fs.HandleWriter(&FileHandle{})

/*
FUSE method that writes to a file handle at a particular offset.
*/
func (fh *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	// fmt.Printf("writing to file with inodeNum: %d\n", fh.inodeNum)

	// this is not very fault tolerant...
	fh.inode.writeToData(req.Data, uint64(req.Offset))
	resp.Size = len(req.Data)
	return nil
}
