---
description: This guide will help you deploy your production lakeFS environment with Docker
---

# Deploy lakeFS on Docker

## Database

lakeFS requires a PostgreSQL database to synchronize actions on your repositories.
This section assumes you already have a PostgreSQL database accessible from where you intend to install lakeFS.
Instructions for creating the database can be found on the deployment instructions for [AWS](./aws.md#creating-the-database-on-aws-rds), [Azure](./azure.md#creating-the-database-on-azure-database) and [GCP](./gcp.md#creating-the-database-on-gcp-sql).

## Installing on Docker

To deploy using Docker, create a yaml configuration file.
Here is a minimal example, but you can see the [reference](../reference/configuration.md#example-aws-deployment) for the full list of configurations.

{% tabs %}

{% tab title="AWS" %}
```yaml
database:
  connection_string: "[DATABASE_CONNECTION_STRING]"
auth:
  encrypt:
    secret_key: "[ENCRYPTION_SECRET_KEY]"
blockstore:
  type: s3
gateways:
  s3:
    domain_name: "[S3_GATEWAY_DOMAIN]"
```
{% endtab %}
{% tab title="Google Cloud" %}
```yaml
database:
  connection_string: "[DATABASE_CONNECTION_STRING]"
auth:
  encrypt:
    secret_key: "[ENCRYPTION_SECRET_KEY]"
blockstore:
  type: gs
# Uncomment the following lines to give lakeFS access to your buckets using a service account:
# gs:
#   credentials_json: [YOUR SERVICE ACCOUNT JSON STRING]
gateways:
  s3:
    domain_name: "[S3_GATEWAY_DOMAIN]"
```
{% endtab %}
{% tab title="Microsoft Azure" %}
```yaml
database:
  connection_string: "postgres://user:pass@<AZURE_POSTGRES_SERVER_NAME>..."
auth:
  encrypt:
    secret_key: "<RANDOM_GENERATED_STRING>"
blockstore:
  type: azure
  azure:
    auth_method: msi # msi for active directory, access-key for access key 
    #  In case you chose to authenticate via access key replace unmark the following rows and insert the values from the previous step 
    #  storage_account: <your storage account>
    #  storage_access_key: <your access key>
gateways:
  s3:
    domain_name: s3.lakefs.example.com
```
{% endtab %}
{% endtabs %}


Save the configuration file locally as `lakefs-config.yaml` and run the following command:

```sh
docker run \
  --name lakefs \
  -p 8000:8000 \
  -v $(pwd)/lakefs-config.yaml:/home/lakefs/.lakefs.yaml \
  treeverse/lakefs:latest run
```

## Load balancing

You should have a load balancer direct requests to the lakeFS server.
By default, lakeFS operates on port 8000, and exposes a `/_health` endpoint which you can use for health checks.

## DNS

As mentioned above, you should create 3 DNS records for lakeFS:
1. One record for the lakeFS API: `lakefs.example.com`
1. Two records for the S3-compatible API: `s3.lakefs.example.com` and `*.s3.lakefs.example.com`.

All records should point to your Load Balancer, preferably with a short TTL value.

## Next Steps

Your next step is to [prepare your storage](../setup/storage/index.md). If you already have a storage bucket/container, you are ready to [create your first lakeFS repository](../setup/create-repo.md).
