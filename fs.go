// adapted from https://blog.gopheracademy.com/advent-2014/fuse-zipfs/ tutorial

package main

import (
	"bazil.org/fuse/fs"
	"container/list"
	"encoding/binary"
	"fmt"
	"strconv"
)

/*
struct representing the FUSE file system.
*/
type FS struct {
	inodeStream *IntStream
	rootInode   uint64
}

var _ fs.FS = (*FS)(nil)

/*
FUSE method that returns a directory corresponding to the root of the file system.
*/
func (f *FS) Root() (fs.Node, error) {
	inode, err := getInode(f.rootInode)
	root := &Dir{
		inode:       inode,
		inodeNum:    f.rootInode,
		inodeStream: f.inodeStream,
	}
	return root, err
}

var _ = fs.FSDestroyer(&FS{})

/*
FUSE method that performs clean up on the file system when it is unmounted. Also called if there
is an interrupt. The method empties the cache and uploads the superblock to S3. If this fails
to execute before program termination, it is likely the table/bucket will become unusable
unless they are manually emptied.
*/
func (f *FS) Destroy() {
	fmt.Println()
	fmt.Println("Beginning file system cleanup.")
	lastInode := f.inodeStream.compressStream()
	lastData := dataStream.compressStream()
	inodeLinkedList, err := f.inodeStream.MarshalBinary()
	if err != nil {
		fmt.Println("VERY BAD ERROR IN inodeStream.MarshalBinary")
	}
	superBlocks := makeSuperblocks(lastInode, lastData, f.rootInode, inodeLinkedList)
	client := getClient()
	for index, block := range superBlocks {
		blockName := S3_SUPERBLOCK_NAME + strconv.Itoa(index)
		err = putDataByKey(client, blockName, block)
		if err != nil {
			fmt.Println("error writing superblock on FS.Destroy: " + err.Error())
		}
	}
	err = cache.empty()
	if err != nil {
		fmt.Println("Error doing cache.empty(): " + err.Error())
	}
	// would call unmount here, but for some reason it hangs for ~20 seconds
	fmt.Println("File system cleanup successful.")
}

/*
Return a pointer to a new FS initialized with values from the super data block
*/
func makeFs(super *DataBlock) *FS {
	// fmt.Println("doing makeFS")
	rootInode := binary.LittleEndian.Uint64(super.Data[16:24])
	listSize := binary.LittleEndian.Uint64(super.Data[24:32])

	inodeStream := new(IntStream)
	var inodeBytes [8]byte
	copy(inodeBytes[:], super.Data[0:8])
	inodeStream.decompressStream(inodeBytes)
	var dataBytes [8]byte
	copy(dataBytes[:], super.Data[8:16])

	// dataStream is declared globally for use by inode methods
	dataStream = new(IntStream)
	dataStream.decompressStream(dataBytes)
	dataStream.stack = new(list.List)

	var readEnd uint64
	if listSize < BLOCK_SIZE-32 {
		readEnd = listSize + 32
	} else {
		readEnd = BLOCK_SIZE
	}
	listData := make([]byte, listSize)
	copy(listData[0:readEnd-32], super.Data[32:readEnd])
	listSize = listSize - (readEnd - 32)
	amountRead := readEnd

	numBlocksNeeded := 1 + (listSize / BLOCK_SIZE)
	client := getClient()
	var i uint64
	for i = 1; i < numBlocksNeeded; i++ {
		key := S3_SUPERBLOCK_NAME + strconv.FormatUint(i, 10)
		block, err := getDataByKey(client, key)
		if err != nil {
			fmt.Printf("VERY BAD ERROR getting superblock number %d\n", i)
		}
		if listSize < BLOCK_SIZE {
			readEnd = listSize
		} else {
			readEnd = BLOCK_SIZE
		}
		copy(listData[amountRead:amountRead+readEnd], block.Data[0:readEnd])
		listSize = listSize - readEnd
		amountRead = amountRead + readEnd
	}

	if listSize > 0 {
		inodeStream.UnmarshalBinary(listData)
	} else {
		inodeStream.stack = new(list.List)
	}
	return &FS{
		inodeStream: inodeStream,
		rootInode:   rootInode,
	}
}

/*
Write data into the super data block. First 8 bytes are the index of the last "allocated" inode,
next 8 are the last "allocated" dataBlock, and the next 8 is the inode number of the root
*/
func makeSuperblocks(inode, data [8]byte, root uint64, inodeListData []byte) []*DataBlock {
	// fmt.Println("doing writeSuperblock")
	super := new(DataBlock)
	inodeListSize := uint64(len(inodeListData))
	rootBuf := make([]byte, 8, 8)
	listSizeBuf := make([]byte, 8, 8)
	binary.LittleEndian.PutUint64(rootBuf, root)
	binary.LittleEndian.PutUint64(listSizeBuf, inodeListSize)
	for i := 0; i < 8; i++ {
		super.Data[i] = inode[i]
		super.Data[i+8] = data[i]
		super.Data[i+16] = rootBuf[i]
		super.Data[i+24] = listSizeBuf[i]
	}
	var writeEnd uint64
	if inodeListSize+32 > BLOCK_SIZE {
		writeEnd = BLOCK_SIZE
	} else {
		writeEnd = inodeListSize + 32
	}
	copy(super.Data[32:writeEnd], inodeListData[0:writeEnd-32])
	inodeListData = inodeListData[writeEnd-32:]
	numBlocksNeeded := 1 + (uint64(len(inodeListData)) / BLOCK_SIZE)
	superBlocks := make([]*DataBlock, numBlocksNeeded)
	superBlocks[0] = super
	var j uint64
	for j = 1; j < numBlocksNeeded; j++ {
		block := new(DataBlock)
		if uint64(len(inodeListData)) > BLOCK_SIZE {
			writeEnd = BLOCK_SIZE
		} else {
			writeEnd = uint64(len(inodeListData))
		}
		copy(block.Data[0:writeEnd], inodeListData[0:writeEnd])
		inodeListData = inodeListData[writeEnd:]
		superBlocks[j] = block
	}
	return superBlocks
}
