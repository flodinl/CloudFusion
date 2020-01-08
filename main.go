// adapted from https://blog.gopheracademy.com/advent-2014/fuse-zipfs/ tutorial

package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"container/list"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
)

const S3_SUPERBLOCK_NAME string = "super"
const ROOT_INODE uint64 = 1 // cannot be set to 0 or things will break
const CONFIG_FILE_NAME string = "CFconfig.json"
const TEST_FLAG = "test"

var S3_BUCKET_NAME string
var S3_REGION string
var DYNAMO_TABLE_NAME string
var progName = filepath.Base(os.Args[0])
var dataStream *IntStream
var cache *Cache
var credentialsProfile string
var mountpoint string
var runTests bool

/*
Prints information on how to format the command line args.
*/
func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", progName)
	fmt.Fprintf(os.Stderr, " %s CONFIG_PATH CACHESIZE (test)\n", progName)
	fmt.Fprintf(os.Stderr, "ex: $GOPATH/bin/CFconfig.json 50 test\n")
	flag.PrintDefaults()
}

/*
Processes command line args and uses them to initialize some globals before calling mount.
*/
func main() {
	log.SetFlags(0)
	log.SetPrefix(progName + ": ")

	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 2 && flag.NArg() != 3 {
		usage()
		os.Exit(2)
	}
	configLocation := flag.Arg(0)
	cacheSize, err := strconv.Atoi(flag.Arg(1))
	if err != nil || cacheSize <= 0 {
		fmt.Println("Invalid argument supplied for the cache size.")
		log.Fatal(err)
	}
	if flag.NArg() > 2 {
		if flag.Arg(2) == TEST_FLAG {
			runTests = true
		} else {
			usage()
			os.Exit(2)
		}
	} else {
		runTests = false
	}
	config := readConfig(configLocation)
	S3_REGION = config.Region
	S3_BUCKET_NAME = config.Bucket
	initializeBucket()
	DYNAMO_TABLE_NAME = config.Table
	cache = initializeCache(cacheSize)
	credentialsProfile = config.Credentials
	mountpoint = config.Mountpoint
	if err := mount(mountpoint); err != nil {
		log.Fatal(err)
	}
}

/*
Does 3 things: initializes persistent things if they do not exist (S3 bucket, DynamoDB table, superblock),
sets up a channel to call FS.Destroy on an interrupt, and serves the file system.
*/
func mount(mountpoint string) error {
	c, err := fuse.Mount(mountpoint)
	if err != nil {
		return err
	}
	defer c.Close()

	client := getClient()

	// fmt.Println("doing getData for superblock")
	superKey := S3_SUPERBLOCK_NAME + "0"
	super, err := getDataByKey(client, superKey)
	if err != nil {
		super = makeNewSuperblock()
	}
	filesys := makeFs(super)
	// fmt.Println("finished makeFs")

	// from http://stackoverflow.com/questions/11268943/golang-is-it-possible-to-capture-a-ctrlc-signal-and-run-a-cleanup-function-in
	c2 := make(chan os.Signal, 1)
	signal.Notify(c2, os.Interrupt)
	signal.Notify(c2, syscall.SIGTERM)
	go func() {
		<-c2
		filesys.Destroy()
		os.Exit(1)
	}()

	_, err = getInode(filesys.rootInode)
	if err != nil {
		makeNewRootInode()
	}

	if runTests {
		fmt.Println("Test flag was set, so running all tests.")
		go runAllTests()
	}

	fmt.Println("File system mounted.")
	if err := fs.Serve(c, filesys); err != nil {
		return err
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		return err
	}

	return nil
}

/*
Constructs and returns a new superblock if one does not exist in the specified S3 bucket.
*/
func makeNewSuperblock() *DataBlock {
	// fmt.Println("error doing getData for superblock")
	var superData [BLOCK_SIZE]byte
	super := &DataBlock{
		Data: superData,
	}
	// this is the easiest way to make streams start at 1, which is needed so that the zero
	// value of a map differs from any inode number... :(
	tempFs := makeFs(super)
	tempFs.inodeStream.lastInt = 1
	tempFs.inodeStream.stack = new(list.List)
	dataStream.lastInt = 1
	lastInode := tempFs.inodeStream.compressStream()
	lastData := dataStream.compressStream()

	inodeListData, err := tempFs.inodeStream.MarshalBinary()
	if err != nil {
		fmt.Println("VERY BAD ERROR marshaling binary from inodeStream in makeNewSuperblock")
	}
	super = makeSuperblocks(lastInode, lastData, ROOT_INODE, inodeListData)[0]
	// fmt.Println("doing makeFs with new blank superblock")
	return super
}

/*
Constructs and returns an inode for the file system root, if one does not
already exist.
*/
func makeNewRootInode() {
	// fmt.Println("error doing get inode for root")
	var isDir int8 = 1
	newRootInode := createInode(isDir)
	newRootInode.init(ROOT_INODE, ROOT_INODE)
	// fmt.Println("created new root inode")
	err2 := putInode(newRootInode, ROOT_INODE)
	if err2 != nil {
		log.Fatal(err2)
	}
	// fmt.Println("uploaded new root inode")
}

/*
Struct used to represent information in CFconfig.json.
*/
type Config struct {
	Region      string
	Bucket      string
	Credentials string
	Mountpoint  string
	Table       string
}

/*
Reads from the config file at the specified path and returns a Config with the AWS region, the S3 bucket name,
the name of the AWS credentials profile, and the desired mountpoint of the file system.
*/
func readConfig(configFilePath string) *Config {
	// fmt.Println("doing readConfig")
	file, err := os.Open(configFilePath)
	defer file.Close()
	decoder := json.NewDecoder(file)
	config := new(Config)
	err = decoder.Decode(config)
	if err != nil {
		log.Fatal(err)
	}
	return config
}

/*
Checks if the specified S3 bucket already exists, and if it does not, attempts to create a new one.
Exits the program on failure, as this is unrecoverable.
*/
func initializeBucket() {
	client := getClient()
	params := &s3.GetBucketLocationInput{
		Bucket: aws.String(S3_BUCKET_NAME), // Required
	}
	_, err := client.GetBucketLocation(params)
	if err != nil {
		// bucket does not exist
		params := &s3.CreateBucketInput{
			Bucket: aws.String(S3_BUCKET_NAME), // Required
		}
		_, err := client.CreateBucket(params)
		if err != nil {
			fmt.Println("Attempted to create bucket with name " + S3_BUCKET_NAME + ", but failed.")
			fmt.Println("Error was: " + err.Error())
			os.Exit(2)
		}
		// fmt.Println("created new bucket with name: " + S3_BUCKET_NAME)
	}
}

/*
Helper function that initializes a client for S3.
*/
func getClient() *s3.S3 {
	var client *s3.S3
	client = s3.New(session.New(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewSharedCredentials("", credentialsProfile),
	}))
	return client
}

/*
Helper function that initializes a client for DynamoDB.
*/
func getDynamoClient() *dynamodb.DynamoDB {
	client := dynamodb.New(session.New(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewSharedCredentials("", credentialsProfile),
	}))
	return client
}
