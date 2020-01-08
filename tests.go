package main

import (
	"container/list"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)

/*
Runs all tests associated with the program. A test failure does not result in a program halt or exit,
because this would interfere with maintaining the state of the bucket/table.
*/
func runAllTests() {
	inodeTableTest()
	streamTest()
	// sleep here so the file system has time be initialized
	time.Sleep(5 * time.Second)
	mkdirTest()
	smallWriteTest()  // tests file that fits in inode buffer
	mediumWriteTest() // tests file that fits in a few data blocks
	largeWriteTest()  // tests file that fits in the singly indirect block
	// veryLargeWriteTest() // tests bigger file in singly indirect. ~8MB, so ~250 put/get/delete reqs

	// doing a test to check writes to the doubly indirect block takes something like ~4000 puts
	// it's probably easier to manually lower the BLOCK_SIZE to check it

	// would be nice to do explicit testing of dynamodb and s3, but not sure how
	fmt.Println("All tests completed.")
}

/*
Tests writing, reading, and deleting a small file (239B) that fits in the inode buffer.
*/
func smallWriteTest() {
	errMessage := writeTest("smallFile.txt")
	if errMessage != "" {
		fmt.Println(errMessage + " in smallWriteTest")
	} else {
		fmt.Println("smallWriteTest passed")
	}
}

/*
Tests writing, reading, and deleting a medium file (120KB) that fits in several data blocks.
*/
func mediumWriteTest() {
	errMessage := writeTest("mediumFile.txt")
	if errMessage != "" {
		fmt.Println(errMessage + " in mediumWriteTest")
	} else {
		fmt.Println("mediumWriteTest passed")
	}
}

/*
Tests writing, reading, and deleting a larger file (420KB) that fits in the beginning of the singly indirect block.
*/
func largeWriteTest() {
	errMessage := writeTest("largeFile.txt")
	if errMessage != "" {
		fmt.Println(errMessage + " in largeWriteTest")
	} else {
		fmt.Println("largeWriteTest passed")
	}
}

/*
Tests writing, reading, and deleting a large file (8MB) that fits in a larger portion of the singly indirect block.
This is slow and fairly expensive, especially if run with a small cache size that requires writes back to S3.
*/
func veryLargeWriteTest() {
	errMessage := writeTest("veryLargeFile.txt")
	if errMessage != "" {
		fmt.Println(errMessage + " in veryLargeWriteTest")
	} else {
		fmt.Println("veryLargeWriteTest passed")
	}
}

/*
Generic test for writing/reading/deleting a single file. First the file is copied into the file
system at the root, then it is read and the number of bytes read is checked against the original file.
Finally, it is deleted from the file system. If the cache size is large enough, nothing will
ever be written back to S3.
*/
func writeTest(fileName string) string {
	goPath := os.Getenv("GOPATH")
	err := os.Chdir(goPath + "/bin/CloudFusionTests")
	file, _ := os.Open(fileName)
	data, _ := ioutil.ReadAll(file)
	_ = file.Close()
	fileLen := len(data)

	path := mountpoint
	newFile, err := os.Create(path + "/" + fileName)
	if err != nil {
		return "error from create"
	}
	_, err = newFile.Write(data)
	if err != nil {
		fmt.Println(err.Error())
		return "error from write"
	}
	err = newFile.Close()
	if err != nil {
		return "error from fileHandle close"
	}
	newFile, err = os.Open(path + "/" + fileName)
	if err != nil {
		return "error from opening new file"
	}
	newData, err := ioutil.ReadAll(newFile)
	if err != nil || len(newData) != fileLen {
		return "error from reading new file"
	}
	err = newFile.Close()
	if err != nil {
		return "error from fileHandle close"
	}
	err = os.RemoveAll(path + "/" + fileName)
	if err != nil {
		return "error from deleting file"
	}
	return ""
}

/*
Unit tests for the IntStream struct that check it's compression/decompression functions
and that it's stack is working correctly.
*/
func streamTest() {
	testStream := &IntStream{
		stack:   new(list.List),
		lastInt: 1,
	}
	nextNum := testStream.next()
	if nextNum != 2 {
		fmt.Println("error from stream.next in streamTest")
	}
	compressedNum := testStream.compressStream()
	testStream.lastInt = 100
	testStream.decompressStream(compressedNum)
	if testStream.lastInt != 2 {
		fmt.Println("error from compress/decompress stream in streamTest")
	}
	testStream.put(29)
	data, err := testStream.MarshalBinary()
	if err != nil {
		fmt.Println("error from stream.MarshalBinary in streamTest")
	}
	testStream.stack = new(list.List)
	err = testStream.UnmarshalBinary(data)
	if err != nil {
		fmt.Println("error from stream.UnmarshalBinary in streamTest")
	}
	nextNum = testStream.next()
	nextNextNum := testStream.next()
	if nextNum != 29 || nextNextNum != 3 {
		fmt.Println("error from stream.next after UnmarshalBinary in streamTest")
	}
	fmt.Println("streamTest passed")
}

/*
Creates and deletes a directory from the root of the file system.
*/
func mkdirTest() {
	var perm os.FileMode = 1 << 31
	path := mountpoint + "/testDir"
	// fmt.Println("path for mkdirTest is " + path)
	err := os.MkdirAll(path, perm)
	if err != nil {
		fmt.Println("error from MkdirAll in mkdirTest")
	} else {
		fmt.Println("mkdirTest passed")
	}
	err = os.RemoveAll(path)
	if err != nil {
		fmt.Println("error from RemoveAll in mkdirTest")
	}
}

/*
Unit testing the inodeTable struct that checks its compression/decompression
functionality.
*/
func inodeTableTest() {
	table := new(InodeTable)
	table.init(1, 27)
	table.add("testFile", 5)
	tableData, err := table.MarshalBinary()
	if err != nil {
		fmt.Println("error from MarshalBinary in inodeTableTest")
	}
	newTable := new(InodeTable)
	err2 := newTable.UnmarshalBinary(tableData)
	if err2 != nil {
		fmt.Println("error from UnmarshalBinary in inodeTableTest")
	}
	if newTable.Table["."] != 27 || newTable.Table["testFile"] != 5 {
		fmt.Println("incorrect values from table in inodeTableTest")
	}
	newTable.delete("testFile")
	if newTable.Table["testFile"] != 0 {
		fmt.Println("table delete failed in inodeTableTest")
	}
	fmt.Println("inodeTableTest passed")
}
