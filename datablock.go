package main

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"strconv"
)

const BLOCK_SIZE uint64 = 32768 // this can be modified as long as it is a multiple of 8 and the inode size

/*
Struct that represents a single data block in the file system.
*/
type DataBlock struct {
	Data [BLOCK_SIZE]byte
}

/*
Gets a DataBlock from S3/DynamoDB by the dataNum.
*/
func getData(dataNum uint64) (*DataBlock, error) {
	// fmt.Printf("doing get data for data id %d\n", dataNum)
	client := getClient()
	key := genDataKey(dataNum)
	// fmt.Println("key for getData is: " + key)
	data, err := getDataByKey(client, key)
	return data, err
}

/*
Returns a DataBlock from S3/DynamoDB containing the inode with given inodeNum. Multiple inodes
are packed into a single block.
*/
func getInodeBlock(inodeNum uint64) (*DataBlock, error) {
	// fmt.Printf("doing get inodeBlock for inode num %d\n", inodeNum)
	client := getClient()
	key := genInodeBlockKey(inodeNum)
	// fmt.Println("doing getInodeBlock for key: " + key)
	data, err := getDataByKey(client, key)
	return data, err
}

/*
Deletes a block with the specified dataNum from both S3 and DynamoDB,
returning an error only if it cannot be found in either one.
*/
func deleteBlock(dataNum uint64) error {
	// fmt.Printf("doing deleteBlock for blockNum: %d\n", dataNum)
	client := getClient()
	key := genDataKey(dataNum)
	cacheErr := cache.deleteBlock(key)
	_, err := client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(S3_BUCKET_NAME),
		Key:    aws.String(key),
	})
	if err != nil && cacheErr != nil {
		return errors.New("Failed to delete from both DynamoDB and S3.")
	}
	return nil
}

/*
Uploads a dataBlock with the specified number.
*/
func putData(dataNum uint64, data *DataBlock) error {
	// fmt.Printf("doing putData for dataBlock with data num %d\n", dataNum)
	client := getClient()
	key := genDataKey(dataNum)
	err := putDataByKey(client, key, data)
	return err
}

/*
Uploads a data block consisting of inodes including the specified inode number.
*/
func putInodeBlock(inodeNum uint64, inodeBlock *DataBlock) error {
	// fmt.Printf("doing putInodeBlock for inodeBlock with inode num %d\n", inodeNum)
	client := getClient()
	key := genInodeBlockKey(inodeNum)
	err := putDataByKey(client, key, inodeBlock)
	return err
}

/*
Uploads a data block to the cache using key as the name of the file to be uploaded.
*/
func putDataByKey(client *s3.S3, key string, data *DataBlock) error {
	// fmt.Println("doing putDataByKey for key: " + key)
	// fmt.Println("doing cache upload in putDataByKey")
	err := cache.addBlock(data, key)
	if err != nil {
		fmt.Println("Error in putDataByKey from cache.addBlock: " + err.Error())
		return err
	}
	return nil
}

/*
Retrieves a data block with the specified key from either DynamoDB or S3. DynamoDB
is tried first (because it is the cache). Returns a new empty data block and an error if such
a file is not found in the standard execution path.
*/
func getDataByKey(client *s3.S3, key string) (*DataBlock, error) {
	var data *DataBlock = new(DataBlock)
	dataSlice, err := cache.getBlock(key)
	if err != nil {
		// cache miss
		// fmt.Println("cache miss trying for key:" + key)
		output, err := client.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(S3_BUCKET_NAME),
			Key:    aws.String(key),
		})
		// fmt.Println("about to try read into data from getDataByKey")
		if err == nil {
			// item existed in s3
			err2 := binary.Read(output.Body, binary.LittleEndian, data)
			if err2 != nil {
				// s3 request succeeded but binary.Read failed (malformed write?)
				fmt.Println("Error doing binary.Read from getObject output in getDataByKey: " + err2.Error())
				return data, err2
			} else {
				// s3 request succeeded
				// add to cache since this was a cache miss
				cache.addBlock(data, key)
				return data, nil
			}
		} else {
			// item not in s3, return a blank data block for writing
			// don't bother adding to cache, because it will
			// be added anyways when written to (this branch should occur only
			// immediately before a write)
			return data, err
		}
	} else {
		// cache hit
		// fmt.Println("cache hit trying for key:" + key)
		copy(data.Data[:], dataSlice)
		return data, nil
	}

}

/*
Inode block keys are of the format "HASH-inodeBlockNUMBER", where HASH is the first 2
bytes of the md5 hash of "inodeNUMBER". Theoretically this allows
for higher throughput on S3 (see
http://docs.aws.amazon.com/AmazonS3/latest/dev/request-rate-perf-considerations.html)
*/
func genInodeBlockKey(inodeNum uint64) string {
	var blockNum uint64 = inodeNum / (BLOCK_SIZE / INODE_SIZE)
	ident := "inodeBlock" + strconv.FormatUint(blockNum, 10)
	h := md5.New()
	io.WriteString(h, ident)
	hash := hex.EncodeToString(h.Sum(nil)[:2])
	// fmt.Println("did genInodeBlockKey, new key is " + hash + "-" + ident)
	return hash + "-" + ident
}

/*
Data keys are of the format "HASH-dataNUMBER", where HASH is the first 2
bytes of the md5 hash of "dataNUMBER". Theoretically this allows
for higher throughput on S3 (see
http://docs.aws.amazon.com/AmazonS3/latest/dev/request-rate-perf-considerations.html)
*/
func genDataKey(dataNum uint64) string {
	ident := "data" + strconv.FormatUint(dataNum, 10)
	h := md5.New()
	io.WriteString(h, ident)
	hash := hex.EncodeToString(h.Sum(nil)[:2])
	// fmt.Println("did genDataKey, new key is " + hash + "-" + ident)
	return hash + "-" + ident
}
