// adapted from https://blog.gopheracademy.com/advent-2014/fuse-zipfs/ tutorial

package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"errors"
	"fmt"
	"golang.org/x/net/context"
	"os"
	"time"
)

/*
Struct representing a directory in the FUSE file system.
*/
type Dir struct {
	inode       *Inode
	inodeNum    uint64
	inodeStream *IntStream
}

var _ fs.Node = (*Dir)(nil)

/*
FUSE method that returns meta data about the directory.
*/
func (d *Dir) Attr(ctx context.Context, attr *fuse.Attr) error {
	// fmt.Printf("getting attr of dir with inode %d\n", d.inodeNum)
	attr.Size = d.inode.Size
	var fileMode os.FileMode = 0
	if d.inode.IsDir == 1 {
		fileMode = 1 << 31
	}
	attr.Mode = fileMode
	fileTime := time.Unix(d.inode.UnixTime, 0)
	attr.Mtime = fileTime
	attr.Ctime = fileTime
	attr.Crtime = fileTime
	return nil
}

var _ fs.Handle = (*DirHandle)(nil)

/*
struct that represents a file handle for a directory in the FUSE file system.
*/
type DirHandle struct {
	inode      *Inode
	inodeTable *InodeTable
	inodeNum   uint64
}

var _ = fs.NodeOpener(&Dir{})

/*
FUSE method that returns a file handle for the relevant directory.
*/
func (d *Dir) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	// fmt.Printf("opening file with inodeNum: %d\n", d.inodeNum)
	var offset uint64 = 0
	tableData, err := d.inode.readFromData(offset, d.inode.Size)
	table := new(InodeTable)
	table.UnmarshalBinary(tableData)
	handle := &DirHandle{
		inode:      d.inode,
		inodeTable: table,
		inodeNum:   d.inodeNum,
	}
	return handle, err
}

var _ fs.HandleReleaser = (*DirHandle)(nil)

/*
FUSE method that closes a file handle for a directory.
*/
func (dh *DirHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	// hopefully this can't have an error
	tableData, _ := dh.inodeTable.MarshalBinary()
	var offset uint64 = 0
	dh.inode.writeToData(tableData, offset)
	err := putInode(dh.inode, dh.inodeNum)
	return err
}

var _ = fs.NodeMkdirer(&Dir{})

/*
FUSE method that makes a new directory in the file system and uploads it.
*/
func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	// fmt.Println("doing Mkdir for dir " + req.Name)
	// req contains an os.FileMode but I think it isn't really relevant in this implementation
	var isDir int8 = 1
	inode := createInode(isDir)
	newInodeNum := d.inodeStream.next()
	inode.init(d.inodeNum, newInodeNum)
	err := putInode(inode, newInodeNum)
	d.addFile(req.Name, newInodeNum)
	newDir := &Dir{
		inodeNum:    newInodeNum,
		inode:       inode,
		inodeStream: d.inodeStream,
	}
	// should newDir be returned if err != nil?
	return newDir, err
}

/*
Helper method that adds a fileName/inodeNum pair to the hash table stored in the directory,
and uploads the directory inode to reflect the change.
*/
func (d *Dir) addFile(name string, inodeNum uint64) {
	var offset uint64 = 0
	data, _ := d.inode.readFromData(offset, d.inode.Size)
	table := new(InodeTable)
	err := table.UnmarshalBinary(data)
	if err != nil {
		fmt.Println("VERY BAD error doing unmarshal binary on table: " + err.Error())
	}
	table.add(name, inodeNum)
	data, err = table.MarshalBinary()
	if err != nil {
		fmt.Println("VERY BAD error doing marshal binary on table: " + err.Error())
	}
	d.inode.writeToData(data, offset)
	putInode(d.inode, d.inodeNum)
}

/*
Removes a file with the given name from the directory's inode table. Not to be confused
with Remove, which actually deletes a file from the file system.
*/
func (d *Dir) removeFile(name string) (uint64, error) {
	var offset uint64 = 0
	data, _ := d.inode.readFromData(offset, d.inode.Size)
	table := new(InodeTable)
	err := table.UnmarshalBinary(data)
	if err != nil {
		fmt.Println("VERY BAD error doing unmarshal binary on table: " + err.Error())
	}
	inodeNum := table.Table[name]
	if inodeNum == 0 {
		// file does not exist in directory
		return 0, fuse.ENOENT
	} else {
		table.delete(name)
	}
	data, err = table.MarshalBinary()
	if err != nil {
		fmt.Println("VERY BAD error doing marshal binary on table: " + err.Error())
	}
	d.inode.writeToData(data, offset)
	putInode(d.inode, d.inodeNum)
	return inodeNum, nil
}

var _ = fs.NodeStringLookuper(&Dir{})

/*
FUSE method that returns a node corresponding to a directory entry in the current directory,
if one exists.
*/
func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	// fmt.Printf("doing lookup of dir at inode %d\n", d.inodeNum)
	var offset uint64 = 0
	tableData, err := d.inode.readFromData(offset, d.inode.Size)
	if err != nil {
		fmt.Println("VERY BAD error doing readFromData from offset 0 in Lookup " + err.Error())
	}
	table := new(InodeTable)
	table.UnmarshalBinary(tableData)
	inodeNum := table.Table[name]
	if inodeNum == 0 {
		return nil, fuse.ENOENT
	} else {
		inode, err := getInode(inodeNum)
		if err != nil {
			fmt.Println("VERY BAD error doing getInode on existing entry in Lookup: " + err.Error())
		}
		var child fs.Node
		if inode.IsDir == 1 {
			child = &Dir{
				inode:       inode,
				inodeNum:    inodeNum,
				inodeStream: d.inodeStream,
			}
		} else {
			child = &File{
				inode:       inode,
				inodeNum:    inodeNum,
				inodeStream: d.inodeStream,
			}
		}
		return child, nil
	}
}

var _ = fs.NodeRenamer(&Dir{})

/*
FUSE method that renames a file in the directory, and potentially moves it to a new directory.
*/
func (d *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDirNode fs.Node) error {
	// fmt.Printf("doing rename on dir with inodeNum: %d, oldName: "+req.OldName+" newName: "+req.NewName+"\n", d.inodeNum)
	newDir := newDirNode.(*Dir)
	// fmt.Printf("newDir has inodeNum: %d\n", newDir.inodeNum)
	inodeNum, err := d.removeFile(req.OldName)
	if err != nil {
		return err
	}
	newDir.addFile(req.NewName, inodeNum)
	return nil
}

var _ = fs.HandleReadDirAller(&DirHandle{})

/*
FUSE method that returns a list of all directory entries in a directory.
*/
func (dh *DirHandle) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	// fmt.Printf("doing readDirAll of dir with inode %d\n", dh.inodeNum)
	var res []fuse.Dirent

	for name, inodeNum := range dh.inodeTable.Table {
		var dirent fuse.Dirent
		dirent.Name = name
		entInode, err := getInode(inodeNum)
		if err != nil {
			fmt.Println("error doing getInode in ReadDirAll: " + err.Error())
		}
		if entInode.IsDir == 1 {
			dirent.Type = fuse.DT_Dir
		} else {
			dirent.Type = fuse.DT_File
		}
		res = append(res, dirent)
	}
	return res, nil
}

/*
Returns the inodeTable struct from unmarshaling the data of the directory's inode
*/
func getTable(inode *Inode) (*InodeTable, error) {
	var offset uint64 = 0
	tableData, err := inode.readFromData(offset, inode.Size)
	table := new(InodeTable)
	table.UnmarshalBinary(tableData)
	return table, err
}

/*
Writes the table struct to the inode's data
*/
func writeTable(table *InodeTable, inode *Inode) error {
	tableData, err := table.MarshalBinary()
	var offset uint64 = 0
	inode.writeToData(tableData, offset)
	return err
}

var _ = fs.NodeRemover(&Dir{})

/*
FUSE method that removes a file from the given directory, deleting it from the file system if
it's LinkCount becomes 0.
*/
func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	// fmt.Printf("doing remove from dir at inode %d\n", d.inodeNum)

	table, _ := getTable(d.inode)
	inodeNum := table.Table[req.Name]
	if inodeNum == 0 {
		return fuse.ENOENT
	}
	inode, err := getInode(inodeNum)
	if err != nil {
		return err
	}
	if req.Dir == true && inode.IsDir == 1 {
		removeTable, err := getTable(inode)
		if err != nil {
			return err
		}
		if len(removeTable.Table) != 2 {
			// dir is not empty
			return errors.New("Cannot remove non-empty directory " + req.Name + ".")
		}
	}
	// fmt.Printf("inode linkCount before decrement is: %d\n", inode.LinkCount)
	inode.LinkCount--
	if inode.LinkCount == 0 {
		// fmt.Println("doing deleteAllData in Remove")
		err = inode.deleteAllData()
		if err != nil {
			fmt.Println("err from deleteAllData is: " + err.Error())
			return err
		}
		// fmt.Printf("doing inodeStream.put for inodeNum: %d\n", inodeNum)
		d.inodeStream.put(inodeNum)
	}
	putInode(inode, inodeNum)
	_, err = d.removeFile(req.Name)
	return err
}

var _ = fs.NodeCreater(&Dir{})

/*
FUSE method that creates a new inode for a file being created in the current directory.
If called on an existing file, the file is simply opened and a handle is returned, it is not
overwritten.
*/
func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	// fmt.Printf("creating file in dir with inode %d\n", d.inodeNum)
	// fmt.Println("name of file to be created is: " + req.Name)
	dirTable, err := getTable(d.inode)
	if err != nil {
		return nil, nil, err
	}
	fileExists := dirTable.Table[req.Name] != 0
	var inode *Inode
	var inodeNum uint64
	if !fileExists {
		// fmt.Println("file does not yet exist in Create")
		var isDir int8 = 0
		inode = createInode(isDir)
		inodeNum = d.inodeStream.next()
		inode.init(d.inodeNum, inodeNum)
		d.addFile(req.Name, inodeNum)
	} else {
		// fmt.Println("file already exists in Create")
		inodeNum = dirTable.Table[req.Name]
		inode, err = getInode(inodeNum)
		if err != nil {
			return nil, nil, err
		}
	}

	child := &File{
		inode:       inode,
		inodeNum:    inodeNum,
		inodeStream: d.inodeStream,
	}
	handle := &FileHandle{
		inode:    inode,
		inodeNum: inodeNum,
	}
	// can any errors happen here?
	return child, handle, nil
}
