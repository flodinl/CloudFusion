package main

import (
	"bytes"
	"encoding"
	"encoding/gob"
)

/*
Struct that holds a map between inodes and file names to be used for directory
operations.
*/
type InodeTable struct {
	Table map[string]uint64
}

/*
Initializes the inode table with entries for itself and its parent. I think this may
not actually be necessary because FUSE may handle these internally.
*/
func (i *InodeTable) init(parentInode, selfInode uint64) {
	i.Table = make(map[string]uint64)
	i.add("..", parentInode)
	i.add(".", selfInode)
}

/*
Adds a fileName/inode pair to the hash table.
*/
func (i *InodeTable) add(fileName string, inode uint64) {
	i.Table[fileName] = inode
}

/*
Deletes a filename/inode pair from the hash table.
*/
func (i *InodeTable) delete(fileName string) {
	delete(i.Table, fileName)
}

var _ = encoding.BinaryMarshaler(&IntStream{})

/*
Returns a binary representation of the inodeTable, to be stored in a directory's data.
*/
func (i *InodeTable) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(i.Table)
	return buf.Bytes(), err
}

/*
Unmarshals the supplied binary into this inodeTable.
*/
func (i *InodeTable) UnmarshalBinary(data []byte) error {
	var buf bytes.Buffer
	buf.Write(data)
	dec := gob.NewDecoder(&buf)
	err := dec.Decode(&i.Table)
	return err
}
