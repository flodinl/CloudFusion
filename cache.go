package main

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"os"
	"time"
)

// this can be varied, 25 is the free limit but throttles when working with bigger files
const READ_WRITE_CAPACITY int64 = 100

type Cache struct {
	cacheCapacity     int
	recentlyUsedQueue *list.List               // stores cache entries so that the front is the least recently used
	keyHash           map[string]*list.Element // maps from file name keys to elements of the queue
}

/*
Initializes the local cache data structure with a maximum capacity of cacheSize, and makes it available globally.
cacheSize cannot be equal to 0, because this would require special casing all the cache functions.
*/
func initializeCache(cacheSize int) *Cache {
	client := getDynamoClient()
	isReady, err := checkTableReady(DYNAMO_TABLE_NAME, client)
	if err != nil {
		_, err := createNewTable(DYNAMO_TABLE_NAME, client)
		if err != nil {
			fmt.Println("Error trying to create DynamoDB table with name: " + DYNAMO_TABLE_NAME + ", but failed")
			fmt.Println("Error was: " + err.Error())
			os.Exit(2)
		}
	} else if !isReady {
		for !isReady {
			time.Sleep(time.Second)
			isReady, _ = checkTableReady(DYNAMO_TABLE_NAME, client)
		}
	}
	cache := &Cache{
		cacheCapacity:     cacheSize,
		keyHash:           make(map[string]*list.Element),
		recentlyUsedQueue: new(list.List),
	}
	return cache
}

/*
Adds a data block to the DynamoDB table. If the block was already in the cache, it is
moved to the back of the eviction queue. Otherwise, a new block is added to the eviction queue,
and the front of the queue is evicted if the queue is full.
*/
func (c *Cache) addBlock(data *DataBlock, key string) error {
	params := &dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"Name": {
				S: aws.String(key),
			},
			"Value": {
				B: data.Data[:],
			},
		},
		TableName: aws.String(DYNAMO_TABLE_NAME),
	}
	client := getDynamoClient()
	_, err := client.PutItem(params)
	if err != nil {
		return err
	} else {
		elt := c.keyHash[key]
		if elt == nil {
			// cache miss, so adding a new block, thus must check capacity
			if c.recentlyUsedQueue.Len() == c.cacheCapacity {
				// cache is full, evict LRU element
				// fmt.Printf("about to evict with queue length: %d, capacity: %d\n", c.recentlyUsedQueue.Len(), c.cacheCapacity)

				evictElt := c.recentlyUsedQueue.Front()
				evictKey := c.recentlyUsedQueue.Remove(evictElt).(string)
				c.keyHash[evictKey] = nil
				c.evictBlock(evictKey)
			}
			// new block previously in cache, so add it at front
			newElt := c.recentlyUsedQueue.PushBack(key)
			c.keyHash[key] = newElt
			return nil
		} else {
			// cache hit, so do not need to check capacity
			// just move block to front
			c.recentlyUsedQueue.MoveToBack(elt)
			return nil
		}
	}

}

/*
Deletes a block from DynamoDB without writing to S3, for use in rm calls. Also removes the block
from the eviction queue.
*/
func (c *Cache) deleteBlock(key string) error {
	// fmt.Println("doing cache.deleteBlock for key: " + key)
	elt := c.keyHash[key]
	if elt == nil {
		return errors.New("Failed to removeBlock from cache.")
	}
	c.recentlyUsedQueue.Remove(elt)
	c.keyHash[key] = nil
	params := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"Name": {
				S: aws.String(key),
			},
		},
		TableName: aws.String(DYNAMO_TABLE_NAME),
	}
	dynamoClient := getDynamoClient()
	_, err := dynamoClient.DeleteItem(params)
	if err != nil {
		fmt.Println("Failed to removeBlock from cache: " + err.Error())
		return errors.New("Failed to removeBlock from cache: " + err.Error())
	}
	return nil
}

/*
Writes the contents of the entire DynamoDB table to S3, and deletes all entries from the DynamoDB table.
*/
func (c *Cache) empty() error {
	for e := c.recentlyUsedQueue.Front(); e != nil; e = e.Next() {
		key := e.Value.(string)
		err := c.evictBlock(key)
		if err != nil {
			return err
		}
	}
	return nil
}

/*
Removes a block from the DynamoDB table and writes it to S3.
*/
func (c *Cache) evictBlock(key string) error {
	// fmt.Println("doing cache.evictBlock for key: " + key)
	params := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"Name": {
				S: aws.String(key),
			},
		},
		TableName:    aws.String(DYNAMO_TABLE_NAME),
		ReturnValues: aws.String(dynamodb.ReturnValueAllOld),
	}
	dynamoClient := getDynamoClient()
	resp, err := dynamoClient.DeleteItem(params)
	if err != nil || resp.Attributes["Value"] == nil {
		fmt.Println("Failed to removeBlock from cache: " + err.Error())
		return errors.New("Failed to removeBlock from cache: " + err.Error())
	}

	data := resp.Attributes["Value"].B

	s3Client := getClient()
	reader := bytes.NewReader(data)
	intPtr := new(int64)
	*intPtr = int64(reader.Len())
	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket:        aws.String(S3_BUCKET_NAME),
		Key:           aws.String(key),
		Body:          reader,
		ContentLength: intPtr,
	})
	return nil
}

/*
Gets the associated data from DynamoDB, and moves the block to the back of the eviction queue. This method returns an error
if the relevant block is not in cache.
*/
func (c *Cache) getBlock(key string) ([]byte, error) {
	elt := c.keyHash[key]
	if elt == nil {
		return nil, errors.New("Error doing GetItem to DynamoDB (cache miss).")
	}

	params := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"Name": {
				S: aws.String(key),
			},
		},
		TableName:      aws.String(DYNAMO_TABLE_NAME),
		ConsistentRead: aws.Bool(true),
	}
	client := getDynamoClient()
	resp, err := client.GetItem(params)
	if err != nil || resp.Item["Value"] == nil {
		return nil, errors.New("Error doing GetItem to DynamoDB on supposed cache hit.")
	}

	c.recentlyUsedQueue.MoveToBack(elt)
	return resp.Item["Value"].B, err
}

/*
Does a DescribeTable request and returns a bool representing whether or not the table's status is ACTIVE.
*/
func checkTableReady(name string, client *dynamodb.DynamoDB) (bool, error) {
	describeParams := &dynamodb.DescribeTableInput{
		TableName: aws.String(DYNAMO_TABLE_NAME), // Required
	}
	resp, err := client.DescribeTable(describeParams)
	if err != nil {
		return false, err
	} else {
		return *resp.Table.TableStatus == dynamodb.TableStatusActive, nil
	}
}

/*
Creates a new table with the name specified from the config file. Hard-coded to use 100 units of read/write
capacity (which is more than the free amount).
*/
func createNewTable(name string, client *dynamodb.DynamoDB) (*dynamodb.CreateTableOutput, error) {
	params := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{ // Required
			{ // Required
				AttributeName: aws.String("Name"),                        // Required
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS), // Required
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{ // Required
			{ // Required
				AttributeName: aws.String("Name"),               // Required
				KeyType:       aws.String(dynamodb.KeyTypeHash), // Required
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{ // Required
			ReadCapacityUnits:  aws.Int64(READ_WRITE_CAPACITY), // Required
			WriteCapacityUnits: aws.Int64(READ_WRITE_CAPACITY), // Required
		},
		TableName: aws.String(name), // Required
	}
	return client.CreateTable(params)
}
