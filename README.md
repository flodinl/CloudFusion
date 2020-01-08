# CloudFusion

Detailed information about the design of the system can be found in "DesignDoc.tex".

# Setup Instructions:

0) Install Go, and setup your GOPATH and Go workspace (with bin, pkg, and src folders).

1) Install FUSE (http://fuse.sourceforge.net/).

2) Clone this repository into WORKSPACE/src/ where WORKSPACE is the path of your Go workspace.

3) Navigate to the project directory and run "go get" to get dependencies and compile the code.

4) Set up your AWS credentials (https://github.com/aws/aws-sdk-go/wiki/configuring-sdk#specifying-credentials). A credentials file that can be used for this purpose is provided.

5) Set up the CFconfig.json file in the repository. Open it with any text editor, and edit the fields as necessary:

Region: The AWS region you are using for S3 and DynamoDB.

Bucket: The name of the bucket in S3 into which the file system will be created. If the name you supply is not an existing bucket, one with the specified name will be created if possible. If not possible, the program will exit.

Credentials: The name of the credentials profile you are using locally (not the IAM account name!). This is the label in square brackets at the top of the "credentials" file. If you have only one set of credentials you are using, leave this as "default".

Mountpoint: The absolute path of the directory you wish to use as the mountpoint for the FUSE file system.

Table: The name to use for the DynamoDB table. A new table will be created if one with this name does not exist.

6) Run "make" from the project directory (this compiles the code and copies the config file to $GOPATH/bin).

7) Run the executable as EXECUTABLE CONFIGPATH CACHESIZE (test), where CONFIGPATH is the path of your config file (if using make, it should be available at $GOPATH/bin/CFconfig.json), CACHESIZE is the desired size of the DynamoDB cache in blocks (32KB to a block), and (test) is an optional parameter (that should just read "test" or be omitted) which if included specifies that tests are to be run once the file system is initialized.

8) When the program is ended (either by an unmount or an interrupt), it will continue running while it does cleanup, moving data from the DynamoDB cache into S3. This cleanup cannot be interrupted, or the superblock and/or cache may be "corrupted," necessitating a manual empty of the S3 bucket and DynamoDB table.

# Known Issues:

In some Linux systems only root has mount privileges. Also, FUSE file systems can only be accessed by the user that mounts them. This means that if root has to be used to mount the file system, only root can interact with it once it is mounted. This is not an issue specific to this program.

There is an inconsistent issue with growing files over the size of the inode buffer (or the edge of a datablock?) that only occurs if the file is grown after the file system is mounted and unmounted (at least on OSX). It is fairly tricky to reproduce and often succeeds even if an error is reported.
