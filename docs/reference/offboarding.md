---
description: The simplest way to migrate away from lakeFS is to copy data from a lakeFS repository to an S3 bucket
---

# Migrating away from lakeFS

## Copying data from a lakeFS repository to an S3 bucket

The simplest way to migrate away from lakeFS is to copy data from a lakeFS repository to an S3 bucket
(or any other object store).

For smaller repositories, this could be done using the [AWS CLI](../integrations/aws_cli.md) or [Rclone](../integrations/rclone.md).
For larger repositories, running [DistCp](https://hadoop.apache.org/docs/current/hadoop-distcp/DistCp.html) with lakeFS as the source is also an option.

