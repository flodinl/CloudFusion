package main

import (
	"bytes"
	"container/list"
	"encoding"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"os"
)

/*
Struct that acts as a stream of integers starting with lastInt + 1.
*/
type IntStream struct {
	stack   *list.List
	lastInt uint64
}

/*
Gets the next int from the stream. If ints have been added using put(),
these are returned first (in a FILO manner).
*/
func (s *IntStream) next() uint64 {
	if s.stack.Len() == 0 {
		s.lastInt++
		return s.lastInt
	} else {
		oldFront := s.stack.Remove(s.stack.Front())
		// fmt.Printf("using old inode num for create: %d\n", oldFront)
		return oldFront.(uint64)
	}
}

/*
Adds an int to the stream's stack to be read next.
*/
func (s *IntStream) put(newInt uint64) {
	s.stack.PushFront(newInt)
}

/*
Somewhat misleadingly named; a lightweight representation of the stream
consisting only of binary data needed to express lastInt. Thus, this does
not capture the state of the stack.
*/
func (s *IntStream) compressStream() [8]byte {
	// fmt.Println("doing compressStream")
	var buf [8]byte
	slice := make([]byte, 8, 8)
	binary.LittleEndian.PutUint64(slice, s.lastInt)
	copy(buf[:], slice[0:8])
	return buf
}

/*
Sets the stream's lastInt to that specified in the buffer.
*/
func (s *IntStream) decompressStream(buf [8]byte) {
	// fmt.Println("doing decompressStream")
	lastInt := binary.LittleEndian.Uint64(buf[0:8])
	s.lastInt = lastInt
}

var _ = encoding.BinaryMarshaler(&IntStream{})

/*
Returns a binary version of the stack of the stream. This does not
include the lastInt, so it must be handled separately using compress/decompress stream.
*/
func (s *IntStream) MarshalBinary() ([]byte, error) {
	listArray := make([]uint64, s.stack.Len())
	var elt *list.Element
	for s.stack.Len() > 0 {
		elt = s.stack.Front()
		listArray[s.stack.Len()-1] = elt.Value.(uint64)
		s.stack.Remove(elt)
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(listArray)
	if err != nil {
		fmt.Println("error in stream marshalBinary: " + err.Error())
		os.Exit(2)
	}
	return buf.Bytes(), err
}

/*
Sets the stack of this stream to be the decoding of the data.
*/
func (s *IntStream) UnmarshalBinary(data []byte) error {
	var buf bytes.Buffer
	buf.Write(data)
	dec := gob.NewDecoder(&buf)
	var listArray []uint64
	err := dec.Decode(&listArray)
	s.stack = new(list.List)
	for _, entry := range listArray {
		s.stack.PushFront(entry)
	}
	if err != nil {
		fmt.Println("error in stream unmarshalBinary: " + err.Error())
		os.Exit(2)
	}
	return err
}
