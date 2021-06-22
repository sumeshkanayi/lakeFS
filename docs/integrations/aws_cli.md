---
description: This section covers how to use the AWS CLI for AWS S3 to access lakeFS.
---

# Using lakeFS with AWS CLI

The [AWS Command Line Interface](https://aws.amazon.com/cli/) (CLI) is a unified tool to manage your AWS services.
With just one tool to download and configure,
you can control multiple AWS services from the command line and automate them through scripts.

We could use the file commands for S3 to access lakeFS

## Configuration

We would like to configure an AWS profile for lakeFS.

In order to configure the lakeFS credentials run:

```bash
aws configure --profile lakefs
```
We will be prompted to enter ```AWS Access Key ID``` , ```AWS Secret Access Key``` 

It should look like this:

```bash
aws configure --profile lakefs
# output:  
# AWS Access Key ID [None]: AKIAIOSFODNN7EXAMPLE    
# AWS Secret Access Key [None]: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
# Default region name [None]: 
# Default output format [None]:
```

## Path convention

When accessing objects in s3 we will need to use the lakeFS path convention `s3://[REPOSITORY]/[BRANCH]/PATH/TO/OBJECT`

## Usage

After configuring the credentials, This is how a command should look:

```bash 
aws s3 --profile lakefs \
  --endpoint-url https://s3.lakefs.example.com \
  ls s3://example-repo/main/example-directory
```

Where endpoint-url should be the same value [configured](../reference/configuration.md#reference)
for `gateways.s3.domain_name`.

We could use an [alias](aws_cli.md#adding-an-alias) to make it shorter and more convenient.

## Examples

### List directory 

```bash 
aws --profile lakefs \
  --endpoint-url https://s3.lakefs.example.com \
  s3 ls s3://example-repo/main/example-directory
```

### Copy from lakeFS to lakeFS


```bash
aws --profile lakefs \
  --endpoint-url https://s3.lakefs.example.com \
  s3 cp s3://example-repo/main/example-file-1 s3://example-repo/main/example-file-2
```

### Copy from lakeFS to a local path

```bash
aws --profile lakefs \
  --endpoint-url https://s3.lakefs.example.com \
  s3 cp s3://example-repo/main/example-file-1 /path/to/local/file
```

### Copy from a local path to lakeFS

```bash
aws --profile lakefs \
  --endpoint-url https://s3.lakefs.example.com \
  s3 cp /path/to/local/file s3://example-repo/main/example-file-1
```
### Delete file 

```bash 
aws --profile lakefs \
  --endpoint-url https://s3.lakefs.example.com \
  s3 rm s3://example-repo/main/example-directory/example-file
```

### Delete directory

```bash 
aws --profile lakefs \
  --endpoint-url https://s3.lakefs.example.com \
  s3 rm s3://example-repo/main/example-directory/ --recursive
```

## Adding an alias

In order to make the command shorter and more convenient we can create an alias:

```bash
alias awslfs='aws --endpoint https://s3.lakefs.example.com --profile lakefs'
```

Now, the ls command using the alias will be:

```bash
awslfs s3 ls s3://example-repo/main/example-directory
```
