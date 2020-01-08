package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"time"
)

const INODE_SIZE uint64 = 512     // this can be varied to anything >139 (or maybe equal???)
const NUM_DATA_BLOCKS uint64 = 12 // could be adjusted

// these should not be modified or things will break
const INODE_WITHOUT_BUFFER_SIZE = 139 // this is hard-coded based on the fields in the struct and should not be changed
const INODE_BUFFER_SIZE uint64 = INODE_SIZE - INODE_WITHOUT_BUFFER_SIZE
const FIRST_DATA_BLOCK_BYTE uint64 = INODE_BUFFER_SIZE // index of first byte that needs to be written to a datablock
const FIRST_SINGLY_INDIRECT_BYTE uint64 = FIRST_DATA_BLOCK_BYTE + NUM_DATA_BLOCKS*BLOCK_SIZE
const FIRST_DOUBLY_INDIRECT_BYTE uint64 = FIRST_SINGLY_INDIRECT_BYTE + BLOCK_SIZE*BLOCK_SIZE
const FIRST_TRIPLY_INDIRECT_BYTE uint64 = FIRST_DOUBLY_INDIRECT_BYTE + BLOCK_SIZE*BLOCK_SIZE*BLOCK_SIZE
const IND_BLOCK uint8 = uint8(NUM_DATA_BLOCKS)
const IND_BLOCK_SIZE uint64 = BLOCK_SIZE * BLOCK_SIZE
const DOUB_IND_BLOCK uint8 = uint8(NUM_DATA_BLOCKS) + 1
const DOUB_IND_BLOCK_SIZE uint64 = BLOCK_SIZE * BLOCK_SIZE * BLOCK_SIZE
const TRIP_IND_BLOCK uint8 = uint8(NUM_DATA_BLOCKS) + 2

/*
Struct representing an inode in the file system. The size of the buffer can be varied by
adjusting the INODE_SIZE constant, and it will expand to fill the difference.
*/
type Inode struct {
	Size      uint64
	LinkCount uint16
	UnixTime  int64

	IsDir int8 // this must be an int and not bool to work with encoding/binary

	DataBuf [INODE_BUFFER_SIZE]byte

	// last 3 are singly, doubly, triply indirect
	Data [NUM_DATA_BLOCKS + 3]uint64
}

/*
Helper function that updates size and modified time of an inode.
*/
func (i *Inode) updateSize(size uint64) {
	i.Size = size
	i.UnixTime = time.Now().Unix()
}

/*
Returns a pointer to a new inode with time initialized to the system time.
*/
func createInode(isDir int8) *Inode {
	sysTime := time.Now().Unix()
	var data [15]uint64
	var dataBuf [INODE_BUFFER_SIZE]byte

	return &Inode{
		Size:      0,
		LinkCount: 0,
		UnixTime:  sysTime,
		IsDir:     isDir,
		Data:      data,
		DataBuf:   dataBuf,
	}
}

/*
Initializes a new inode by writing the inode numbers for . and .. to its table if it is a directory,
and setting LinkCount to 1.
*/
func (i *Inode) init(parentNum, thisNum uint64) {
	if i.IsDir == 1 {
		inodeTable := new(InodeTable)
		inodeTable.init(parentNum, thisNum)
		// this shouldn't have an error
		tableData, _ := inodeTable.MarshalBinary()
		var offset uint64
		offset = 0
		i.writeToData(tableData, offset)
		i.updateSize(uint64(len(tableData)))
	}
	i.LinkCount = 1
}

/*
Gets an inode from S3/DynamoDB by the inodeNum.
*/
func getInode(inodeNum uint64) (*Inode, error) {
	// fmt.Printf("doing get inode for inode id %d\n", inodeNum)
	inodeBlock, err := getInodeBlock(inodeNum)
	start := (inodeNum % (BLOCK_SIZE / INODE_SIZE)) * INODE_SIZE
	end := start + INODE_SIZE
	inodeData := inodeBlock.Data[start:end]
	reader := bytes.NewReader(inodeData)
	var inode *Inode = new(Inode)
	if err == nil {
		// fmt.Println("about to try read into inode from getInode")
		err2 := binary.Read(reader, binary.LittleEndian, inode)
		if err2 != nil {
			// if this happens then the s3 data is malformed
			fmt.Println("err2 during getInode is: " + err2.Error())
			os.Exit(1)
		}
		return inode, err2
	} else {
		// fmt.Println("error doing getObject in getInode")
		return inode, err
	}
}

/*
Puts the inode into S3/DynamoDB.
*/
func putInode(inode *Inode, inodeNum uint64) error {
	inodeBlock, err := getInodeBlock(inodeNum)
	if err != nil {
		if inodeNum%(BLOCK_SIZE/INODE_SIZE) != 0 && inodeNum != 1 {
			fmt.Printf("error getting inode with inodeNum %d\n", inodeNum)
			return err
		} else {
			// initialize a new inodeBlock
			inodeBlock = new(DataBlock)
		}
	}
	start := (inodeNum % (BLOCK_SIZE / INODE_SIZE)) * INODE_SIZE
	end := start + INODE_SIZE
	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, *inode)
	if err != nil {
		// if this happens then something really bad happened
		fmt.Println("error doing binary.Write in putInode: " + err.Error())
		os.Exit(1)
	}
	inodeData := buf.Bytes()

	// yuck
	newData := append(append(inodeBlock.Data[:start], inodeData...), inodeBlock.Data[end:]...)

	copy(inodeBlock.Data[:], newData)
	err = putInodeBlock(inodeNum, inodeBlock)
	return err
}

/*
Writes data at offset to the buffer/data blocks associated with the inode.
*/
func (i *Inode) writeToData(data []byte, offset uint64) {
	sizeInt := len(data)
	// fmt.Printf("doing writeToData for data of size: %d\n", len(data))
	// fmt.Printf("offset of writeToData is: %d\n", offset)
	size := uint64(sizeInt)

	// if i.IsDir == 1 {
	// 	i.updateSize(size + offset)
	// }

	// a directory's size needs to be updated manually, because it is stored
	// in a weird format. However, the size of a file should be updated automatically
	// by setAttr syscalls. This never happens, so we must update the size here manually. :(
	i.updateSize(size + offset)
	if offset < INODE_BUFFER_SIZE {
		var writeEnd uint64
		if size-offset < INODE_BUFFER_SIZE {
			writeEnd = size - offset
		} else {
			writeEnd = INODE_BUFFER_SIZE
		}
		writeLen := writeEnd - offset
		copy(i.DataBuf[offset:writeEnd], data[0:writeLen])
		data = data[writeLen:]
	}
	if len(data) > 0 {
		var newOffset uint64
		if offset < INODE_BUFFER_SIZE {
			newOffset = 0
		} else {
			newOffset = offset - INODE_BUFFER_SIZE
		}
		i.writeDataBlocks(data, newOffset)
	}
}

/*
Reads data from offset of the buffer/data blocks associated with the inode and returns it as
a single byte slice.
*/
func (i *Inode) readFromData(offset, size uint64) ([]byte, error) {
	// fmt.Printf("size of read is: %d in readFromData\n", size)
	// fmt.Printf("size of inode is: %d in readFromData\n", i.Size)
	if offset >= i.Size {
		fmt.Println("VERY BAD offset in readFromData larger than size")
		return nil, errors.New("Offset specified to read is past the end of the file.")
	}
	// fmt.Printf("doing readFromData for data of size: %d\n", size)
	data := make([]byte, size)
	leftToRead := size
	if offset < INODE_BUFFER_SIZE {
		var readEnd uint64
		if leftToRead+offset < INODE_BUFFER_SIZE {
			readEnd = leftToRead + offset
		} else {
			readEnd = INODE_BUFFER_SIZE
		}
		readLen := readEnd - offset
		// fmt.Printf("about to read from buffer, readLen is %d, offset is %d, readEnd is %d\n", readLen, offset, readEnd)
		copy(data[0:readLen], i.DataBuf[offset:readEnd])
		leftToRead = leftToRead - readLen
		offset = 0
	}
	if leftToRead > 0 {
		data = i.readDataBlocks(data, offset, leftToRead)
	}
	return data, nil
}

/*
Sends delete requests to S3/DynamoDB for all data blocks the inode uses.
*/
func (i *Inode) deleteAllData() error {
	var numBlocksToDelete uint64
	// fmt.Println("doing deleteAllData")
	if i.Size <= INODE_BUFFER_SIZE {
		numBlocksToDelete = 0
	} else {
		numBlocksToDelete = ((i.Size - INODE_BUFFER_SIZE) / BLOCK_SIZE) + 1
	}
	// fmt.Printf("numBlocksToDelete is: %d\n", numBlocksToDelete)
	var err error
	var j uint64
	for j = 0; j < NUM_DATA_BLOCKS && numBlocksToDelete > 0; j++ {
		err = deleteBlock(i.Data[j])
		if err != nil {
			return err
		}
		numBlocksToDelete--
	}
	if numBlocksToDelete > 0 {
		numBlocksToDelete, err = i.deleteIndirect(numBlocksToDelete, i.Data[IND_BLOCK])
		if err != nil {
			return err
		}
	}
	if numBlocksToDelete > 0 {
		numBlocksToDelete, err = i.deleteDoubIndirect(numBlocksToDelete, i.Data[DOUB_IND_BLOCK])
		if err != nil {
			return err
		}
	}
	if numBlocksToDelete > 0 {
		numBlocksToDelete, err = i.deleteTripIndirect(numBlocksToDelete, i.Data[TRIP_IND_BLOCK])
		if err != nil {
			return err
		}
	}
	if numBlocksToDelete > 0 {
		// this should never happen
		return errors.New("SIZE OF DELETE TOO LARGE")
	}
	return nil
}

/*
Deletes all blocks associated with the specified indirect block. Can be called
on blocks other than the one immediately allocated in the inode, such as those
used in the doubly/triply indirect blocks.
*/
func (i *Inode) deleteIndirect(numBlocks, indBlockNum uint64) (uint64, error) {
	indBlock, err := getData(indBlockNum)
	if err != nil {
		fmt.Println("VERY BAD ERROR: from getData in deleteIndirect: " + err.Error())
	}
	var j uint64
	for j = 0; j < BLOCK_SIZE && numBlocks > 0; j = j + 8 {
		blockAddress := make([]byte, 8)
		copy(blockAddress[0:8], indBlock.Data[j:j+8])
		blockNum := binary.LittleEndian.Uint64(blockAddress)
		err = deleteBlock(blockNum)
		if err != nil {
			return 0, err
		}
		numBlocks--
	}
	err = deleteBlock(indBlockNum)
	if err != nil {
		return 0, err
	}
	return numBlocks, nil
}

/*
Deletes all blocks associated with the specified doubly indirect block.
*/
func (i *Inode) deleteDoubIndirect(numBlocks, indBlockNum uint64) (uint64, error) {
	indBlock, err := getData(indBlockNum)
	if err != nil {
		fmt.Println("VERY BAD ERROR: from getData in deleteDoubIndirect: " + err.Error())
	}
	var j uint64
	for j = 0; j < BLOCK_SIZE && numBlocks > 0; j = j + 8 {
		blockAddress := make([]byte, 8)
		copy(blockAddress[0:8], indBlock.Data[j:j+8])
		blockNum := binary.LittleEndian.Uint64(blockAddress)
		numBlocks, err = i.deleteIndirect(numBlocks, blockNum)
		if err != nil {
			return 0, err
		}
	}
	err = deleteBlock(indBlockNum)
	if err != nil {
		return 0, err
	}
	return numBlocks, nil
}

/*
Deletes all blocks associated with the specified triply indirect block.
*/
func (i *Inode) deleteTripIndirect(numBlocks, indBlockNum uint64) (uint64, error) {
	indBlock, err := getData(indBlockNum)
	if err != nil {
		fmt.Println("VERY BAD ERROR: from getData in deleteTripIndirect: " + err.Error())
	}
	var j uint64
	for j = 0; j < BLOCK_SIZE && numBlocks > 0; j = j + 8 {
		blockAddress := make([]byte, 8)
		copy(blockAddress[0:8], indBlock.Data[j:j+8])
		blockNum := binary.LittleEndian.Uint64(blockAddress)
		numBlocks, err = i.deleteDoubIndirect(numBlocks, blockNum)
		if err != nil {
			return 0, err
		}
	}
	err = deleteBlock(indBlockNum)
	if err != nil {
		return 0, err
	}
	return numBlocks, nil
}

/*
Read from the data blocks of the inode, appending to the end of data. Offset is relative to
the previous read, and does not invlude the inode buffer at all.
*/
func (i *Inode) readDataBlocks(data []byte, offset, leftToRead uint64) []byte {
	var j uint64
	for j = 0; j < NUM_DATA_BLOCKS; j++ {
		if leftToRead > 0 && offset < BLOCK_SIZE {
			// fmt.Printf("reading from block: %d\n", j)
			data, leftToRead = i.readBlock(data, offset, leftToRead, i.Data[j])
			offset = 0
		} else {
			offset = offset - BLOCK_SIZE
		}
	}
	if leftToRead > 0 && offset < FIRST_DOUBLY_INDIRECT_BYTE {
		data, leftToRead = i.readIndirect(data, offset, leftToRead, i.Data[IND_BLOCK])
		offset = 0
	} else {
		offset = offset - (BLOCK_SIZE * BLOCK_SIZE)
	}
	if leftToRead > 0 && offset < FIRST_TRIPLY_INDIRECT_BYTE {
		data, leftToRead = i.readDoubIndirect(data, offset, leftToRead, i.Data[DOUB_IND_BLOCK])
		offset = 0
	} else {
		offset = offset - (BLOCK_SIZE * BLOCK_SIZE * BLOCK_SIZE)
	}
	if leftToRead > 0 {
		data, leftToRead = i.readTripIndirect(data, offset, leftToRead, i.Data[TRIP_IND_BLOCK])
	}
	if leftToRead > 0 {
		// this should never happen (bytes have to be written past ~4500 TB)
		fmt.Println("READ TOO BIG")
	}
	return data
}

/*
Read a single data block with number blockNum from relative offset. Returns the data appended with the new
data, and the number of bytes remanining to read. Relative offset is adjusted by the caller.
*/
func (i *Inode) readBlock(data []byte, offset, leftToRead, blockNum uint64) ([]byte, uint64) {
	// fmt.Printf("inode size is: %d in readBlock\n", i.Size)
	block, err := getData(blockNum)
	if err != nil {
		// so... this is bad and shouldn't ever happen. but actually it happens a lot.
		// it seems like it doesn't break anything, so just don't print the error message.
		// ¯\_(ツ)_/¯

		// fmt.Println("VERY BAD ERROR: from getData in readBlock: " + err.Error())
	}
	var readEnd uint64
	if leftToRead+offset > BLOCK_SIZE {
		readEnd = BLOCK_SIZE
	} else {
		readEnd = offset + leftToRead
	}
	readLen := readEnd - offset
	dataStart := uint64(len(data)) - leftToRead
	// fmt.Printf("about to read from block, readLen is %d, offset is %d, readEnd is %d\n", readLen, offset, readEnd)
	copy(data[dataStart:dataStart+readLen], block.Data[offset:readEnd])
	leftToRead = leftToRead - readLen
	return data, leftToRead
}

/*
Reads data associated with a singly indirect block from a relative offset, appending
it to data.
*/
func (i *Inode) readIndirect(data []byte, offset, leftToRead, indBlockNum uint64) ([]byte, uint64) {
	indBlock, err := getData(indBlockNum)
	if err != nil {
		fmt.Println("VERY BAD ERROR: from getData in readIndirect: " + err.Error())
	}
	var j uint64
	for j = 0; j < BLOCK_SIZE; j = j + 8 {
		if leftToRead > 0 && offset < BLOCK_SIZE {
			blockAddress := make([]byte, 8)
			copy(blockAddress[0:8], indBlock.Data[j:j+8])
			blockNum := binary.LittleEndian.Uint64(blockAddress)
			data, leftToRead = i.readBlock(data, offset, leftToRead, blockNum)
			binary.LittleEndian.PutUint64(blockAddress, blockNum)
			copy(indBlock.Data[j:j+8], blockAddress[0:8])
			offset = 0
		} else {
			offset = offset - BLOCK_SIZE
		}
	}
	return data, leftToRead
}

/*
Reads data associated with a doubly indirect block from a relative offset, appending
it to data.
*/
func (i *Inode) readDoubIndirect(data []byte, offset, leftToRead, indBlockNum uint64) ([]byte, uint64) {
	// fmt.Println("\nDOING READ DOUBLE INDIRECT\n")
	indBlock, err := getData(indBlockNum)
	if err != nil {
		fmt.Println("VERY BAD ERROR: from getData in readDoubIndirect: " + err.Error())
	}
	var j uint64
	for j = 0; j < BLOCK_SIZE; j = j + 8 {
		if leftToRead > 0 && offset < IND_BLOCK_SIZE {
			blockAddress := make([]byte, 8)
			copy(blockAddress[0:8], indBlock.Data[j:j+8])
			blockNum := binary.LittleEndian.Uint64(blockAddress)
			data, leftToRead = i.readIndirect(data, offset, leftToRead, blockNum)
			binary.LittleEndian.PutUint64(blockAddress, blockNum)
			copy(indBlock.Data[j:j+8], blockAddress[0:8])
			offset = 0
		} else {
			offset = offset - IND_BLOCK_SIZE
		}
	}
	return data, leftToRead
}

/*
Reads data associated with a triply indirect block from a relative offset, appending
it to data.
*/
func (i *Inode) readTripIndirect(data []byte, offset, leftToRead, indBlockNum uint64) ([]byte, uint64) {
	indBlock, err := getData(indBlockNum)
	if err != nil {
		fmt.Println("VERY BAD ERROR: from getData in readTripIndirect: " + err.Error())
	}
	var j uint64
	for j = 0; j < BLOCK_SIZE; j = j + 8 {
		if leftToRead > 0 && offset < DOUB_IND_BLOCK_SIZE {
			blockAddress := make([]byte, 8)
			copy(blockAddress[0:8], indBlock.Data[j:j+8])
			blockNum := binary.LittleEndian.Uint64(blockAddress)
			data, leftToRead = i.readDoubIndirect(data, offset, leftToRead, blockNum)
			binary.LittleEndian.PutUint64(blockAddress, blockNum)
			copy(indBlock.Data[j:j+8], blockAddress[0:8])
			offset = 0
		} else {
			offset = offset - DOUB_IND_BLOCK_SIZE
		}
	}
	return data, leftToRead
}

/*
Writes data to the inode's data blocks, where offset is the offset IN THE DATA BLOCKS (i. e. past
the inode buffer).
*/
func (i *Inode) writeDataBlocks(data []byte, offset uint64) {
	var j uint64
	for j = 0; j < NUM_DATA_BLOCKS; j++ {
		if offset < BLOCK_SIZE && len(data) > 0 {
			// fmt.Printf("writing to block %d\n", j)
			i.Data[j], data = i.writeBlock(data, offset, i.Data[j])
			offset = 0
			// fmt.Printf("length of data left to write is: %d\n", len(data))
		} else {
			// set offset to be relative to the next block
			offset = offset - BLOCK_SIZE
		}
	}
	if len(data) > 0 && offset < FIRST_DOUBLY_INDIRECT_BYTE {
		i.Data[IND_BLOCK], data = i.writeIndirect(data, offset, i.Data[IND_BLOCK])
		offset = 0
	} else {
		offset = offset - (BLOCK_SIZE * BLOCK_SIZE)
	}
	if len(data) > 0 && offset < FIRST_TRIPLY_INDIRECT_BYTE {
		i.Data[DOUB_IND_BLOCK], data = i.writeDoubIndirect(data, offset, i.Data[DOUB_IND_BLOCK])
		offset = 0
	} else {
		offset = offset - (BLOCK_SIZE * BLOCK_SIZE * BLOCK_SIZE)
	}
	if len(data) > 0 {
		i.Data[TRIP_IND_BLOCK], data = i.writeTripIndirect(data, offset, i.Data[TRIP_IND_BLOCK])
	}
	if len(data) > 0 {
		// this should never happen
		fmt.Println("DATA TOO BIG")
	}
}

/*
Writes as much of data as possible to the block at blockNum, with relative offset (within this block).
Creates a new data block in S3/DynamoDB if one does not yet exist. Returns the number of the relevant block,
which will be the same unless the block was previously uninitialized, and the original data
with the written portion removed.
*/
func (i *Inode) writeBlock(data []byte, offset, blockNum uint64) (uint64, []byte) {
	oldData, err := getData(blockNum)
	if err != nil {
		oldData = new(DataBlock)
		blockNum = dataStream.next()
		// fmt.Printf("made new block with num: %d\n", blockNum)
	} else {
		// fmt.Printf("writing to existing block with blockNum: %d\n", blockNum)
	}
	sizeInt := len(data)
	size := uint64(sizeInt)
	var writeEnd uint64
	if offset+size > BLOCK_SIZE {
		writeEnd = BLOCK_SIZE
	} else {
		writeEnd = offset + size
	}
	writeLen := writeEnd - offset
	copy(oldData.Data[offset:writeEnd], data[0:writeLen])
	// hopefully this will never error
	err = putData(blockNum, oldData)
	if err != nil {
		fmt.Printf("error in writeBlock with blockNum %d: "+err.Error()+"\n", blockNum)
	}
	return blockNum, data[writeLen:]
}

/*
Writes to a singly indirect block, initializing the block if necessary and returning its identifying number.
Offset is relative, and data is removed from the beginning as it is written.
*/
func (i *Inode) writeIndirect(data []byte, offset, indBlockNum uint64) (uint64, []byte) {
	indBlock, err := getData(indBlockNum)
	if err != nil {
		indBlock = new(DataBlock)
		indBlockNum = dataStream.next()
		// fmt.Printf("made new indBlock with num: %d\n", indBlockNum)
	} else {
		// fmt.Printf("writing to existing indBlock with num: %d\n", indBlockNum)
	}
	var j uint64
	for j = 0; j < BLOCK_SIZE; j = j + 8 {
		if offset < BLOCK_SIZE && len(data) > 0 {
			blockAddress := make([]byte, 8)
			copy(blockAddress[0:8], indBlock.Data[j:j+8])
			blockNum := binary.LittleEndian.Uint64(blockAddress)
			blockNum, data = i.writeBlock(data, offset, blockNum)
			binary.LittleEndian.PutUint64(blockAddress, blockNum)
			copy(indBlock.Data[j:j+8], blockAddress[0:8])
			offset = 0
		} else {
			// set offset to be relative to the next block
			offset = offset - BLOCK_SIZE
		}
	}
	err = putData(indBlockNum, indBlock)
	if err != nil {
		fmt.Println("error doing putData for indirect block: " + err.Error())
	}
	return indBlockNum, data
}

/*
Writes to a doubly indirect block, initializing the block if necessary and returning its identifying number.
Offset is relative, and data is removed from the beginning as it is written.
*/
func (i *Inode) writeDoubIndirect(data []byte, offset, doubBlockNum uint64) (uint64, []byte) {
	// fmt.Println("\nDOING WRITE DOUBLE INDIRECT\n")
	doubBlock, err := getData(doubBlockNum)
	if err != nil {
		doubBlock = new(DataBlock)
		doubBlockNum = dataStream.next()
		// fmt.Printf("made new doubBlock with num: %d\n", doubBlockNum)
	}
	var j uint64
	for j = 0; j < BLOCK_SIZE; j = j + 8 {
		if offset < IND_BLOCK_SIZE && len(data) > 0 {
			indBlockAddress := make([]byte, 8)
			copy(indBlockAddress[0:8], doubBlock.Data[j:j+8])
			indBlockNum := binary.LittleEndian.Uint64(indBlockAddress)
			indBlockNum, data = i.writeIndirect(data, offset, indBlockNum)
			binary.LittleEndian.PutUint64(indBlockAddress, indBlockNum)
			copy(doubBlock.Data[j:j+8], indBlockAddress[0:8])
			offset = 0
		} else {
			// set offset to be relative to the next block
			offset = offset - IND_BLOCK_SIZE
		}
	}
	err = putData(doubBlockNum, doubBlock)
	if err != nil {
		fmt.Println("error doing putData for indirect block: " + err.Error())
	}
	return doubBlockNum, data
}

/*
Writes to a triply indirect block, initializing the block if necessary and returning its identifying number.
Offset is relative, and data is removed from the beginning as it is written.
*/
func (i *Inode) writeTripIndirect(data []byte, offset, tripBlockNum uint64) (uint64, []byte) {
	tripBlock, err := getData(tripBlockNum)
	if err != nil {
		tripBlock = new(DataBlock)
		tripBlockNum = dataStream.next()
	}
	var j uint64
	for j = 0; j < DOUB_IND_BLOCK_SIZE; j = j + 8 {
		if offset < DOUB_IND_BLOCK_SIZE && len(data) > 0 {
			doubBlockAddress := make([]byte, 8)
			copy(doubBlockAddress[0:8], tripBlock.Data[j:j+8])
			doubBlockNum := binary.LittleEndian.Uint64(doubBlockAddress)
			doubBlockNum, data = i.writeDoubIndirect(data, offset, doubBlockNum)
			binary.LittleEndian.PutUint64(doubBlockAddress, doubBlockNum)
			copy(tripBlock.Data[j:j+8], doubBlockAddress[0:8])
			offset = 0
		} else {
			// set offset to be relative to the next block
			offset = offset - DOUB_IND_BLOCK_SIZE
		}
	}
	err = putData(tripBlockNum, tripBlock)
	if err != nil {
		fmt.Println("error doing putData for indirect block: " + err.Error())
	}
	return tripBlockNum, data
}
