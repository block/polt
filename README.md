# polt
Polt is a tool designed to streamline the process of archiving data from Aurora MySQL tables.
It offers efficient and safe data transfer to various destinations, including another table in the same database,
AWS S3, or permanent deletion (aka sending data to /dev/null).

## Prerequesites
- MySQL 8.0

## License
The primary license for polt is [Apache 2.0](./LICENSE), however some files are licensed under MPL 2.0 (as indicated in their SPDX headers).

## Local Development & Testing
See [DEVELOPMENT.md](DEVELOPMENT.md) for instructions on how to run tests with dbdeployer or docker.

## 2 modes
Polt moves data in 2 modes.
- Stage
  - Moves data from source table to an internal staging table in the same database.
- Archive
  - Moves data from staging table to final destination.

Every archive operation will run the 2 modes serially to move the data to final destination.
As stated above `archive` mode doesn't cause any load on original source table as it only works on staging table.

## Archive Support
Currently, Polt supports table and S3 archive destinations. For S3 files are written in parquet format using SNAPPY compression.
For more details about parquet format , see [Parquet File Format](https://parquet.apache.org/docs/file-format/) and [more explanation](https://cloudsqale.com/2020/05/29/how-parquet-files-are-written-row-groups-pages-required-memory-and-flush-operations/)


## S3 archive requirements & limitations
- IAM role with S3 write access should have the below policy permissions.

```terraform
    statement {
      effect = "Allow"
      actions = [
      "s3:PutObject",
      ]
      resources = [
        "arn:aws:s3:::{bucket}/*",
      ]
    }
    statement {
      effect = "Allow"
      actions = [
      "kms:GenerateDataKey",
      ]
      resources = [
        "{bucket_kms_key_arn}",
      ]
   }
```
-  S3 bucket destination path should be in the format of `{bucket}/{prefix}` or `s3://{bucket}/{prefix}`. For example, `my-bucket/prefix/`.
-  Currently, Polt only supports S3 archive destination with parquet format. There is [no official support](https://github.com/apache/iceberg-go?tab=readme-ov-file#readwrite-data-support) for directly writing to Iceberg tables in S3 yet.
-  If the S3 archive job fails, it is resumable , but there might be some data duplication in the destination due to the nature of chunking and the checkpointing mechanism. Future versions may address this issue. But until then you can refer to this example Glue(spark) job to remove duplicates from parquet files and insert to iceberg table. [Remove Duplicates from Parquet Files](examples/remove_duplicates_from_parquet_files.py)

## Dependencies
* Relies on [mysql-client-driver](https://github.com/go-sql-driver/mysql) for connecting to MySQL databases.
* Relies on [Spirit](https://github.com/cashapp/spirit) for chunking the data that needs to be archived and also for many other utilities related to db connection, loading table metadata etc.