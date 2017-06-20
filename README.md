# Kafka Connect S3

[![CircleCI](https://circleci.com/gh/spredfast/kafka-connect-s3.svg?style=shield)](https://circleci.com/gh/spredfast/kafka-connect-s3)

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.spredfast.kafka.connect.s3/kafka-connect-s3/badge.svg?style=plastic)](https://maven-badges.herokuapp.com/maven-central/com.spredfast.kafka.connect.s3/kafka-connect-s3)

This is a [kafka-connect](http://kafka.apache.org/documentation.html#connect) sink and source for Amazon S3, but without any dependency on HDFS/hadoop libs or data formats.

Key Features:

 * Block GZip output - This keeps storage costs low.
 * Accurately reflects source topic - The original partition and offset of your records are recorded in S3. This allows you to:
 * Easily read from a specific topic and partition - Index files make reading a particular offset very efficient, so you only have to download the data that you need.
 * Seek to a date & time - Your bucket will be broken into daily prefixes, which makes it possible to find data that was written around a specific date and time.

## Spredfast Fork

This is a hard fork of the [S3 Sink created by DeviantArt](https://github.com/DeviantArt/kafka-connect-s3).

Notable differences:
 * Requires Java 8+
 * Requires Kafka 0.10.0+
 * Supports Binary and Custom Output Formats
 * Provides a Source for reading data back from S3
 * Repackaged and built with Gradle

We are very grateful to the DeviantArt team for their original work.
We made the decision to hard fork when it became clear that we would be responsible for ongoing maintenance.

## Changelog

 * 0.4.0
 	 * BREAKING CHANGE: Changed the way S3Source offsets are stored to accommodate multiple topics in the same
 	   day prefix. Not compatible with old offsets.

## Usage

***NOTE***: You want to use the shadow jar produced by this project.
As a gradle dependency, it is `com.spredfast.kafka.connect.s3:kafka-connect-s3:0.4.0:shadow`.
The shadow jar ensures there are no conflicts with other libraries.

Use just like any other Connector: add it to the Connect classpath and configure a task. Read the rest of this document for configuration details.

## Important Configuration

Only bytes may be written to S3, so you *must* configure the Sink & Source connectors to "convert" to bytes.

### Worker vs. Connector Settings

There are important settings for your Kafka Connect cluster itself, and important settings for individual connectors.
The following puts cluster settings in `connect-worker.properties` and individual Connector settings in their respective files.

### Recommended Worker Configs

connect-worker.properties:

    # too many records can overwhelm the poll loop on large topics and will result in
    # Connect continously rebalancing without making progress
    consumer.max.poll.records=500
    # Flushing to S3 can take some time, so allow for more than the default 5 seconds when shutting down.
    task.shutdown.graceful.timeout.ms=30000

### 0.10.1.0+

Connect 0.10.1.0 introduced the ability to specify converters at the connector level, so you should specify the `AlreadyBytesConverter` for both the sink and source.

connect-s3/sink.properties:

    key.converter=com.spredfast.kafka.connect.s3.AlreadyBytesConverter
    value.converter=com.spredfast.kafka.connect.s3.AlreadyBytesConverter

### Pre 0.10.1.0

On older Connect versions only the worker [key and value converters](http://docs.confluent.io/2.0.0/connect/userguide.html#common-worker-configs) can be configured, so to get raw bytes for S3 you must either:

 1. Configure your cluster converter to leave records as raw bytes:

     connect-worker.properties:

        key.converter=com.spredfast.kafka.connect.s3.AlreadyBytesConverter
        value.converter=com.spredfast.kafka.connect.s3.AlreadyBytesConverter

 2. Provide the S3 connector with the same converter (to reverse the process.) e.g.,

     connect-worker.properties:

        key.converter=org.apache.kafka.connect.json.JsonConverter
        value.converter=org.apache.kafka.connect.json.JsonConverter

     connect-s3-sink/sink.properties:

        key.converter=org.apache.kafka.connect.json.JsonConverter
        value.converter=org.apache.kafka.connect.json.JsonConverter

See the [wiki](https://github.com/spredfast/kafka-connect-s3/wiki) for further details.

## Build and Run

You should be able to build this with `./gradlew shadowJar`. Once the jar is generated in build/libs, include it in `CLASSPATH` (e.g., export `CLASSPATH=.:$CLASSPATH:/fullpath/to/kafka-connect-s3-jar` )

Run: `bin/connect-standalone.sh  example-connect-worker.properties example-connect-s3-sink.properties`(from the root directory of project, make sure you have kafka on the path, if not then give full path of kafka before `bin`)

## S3 File Format

The default file format is a UTF-8, newline delimited text file. This works well for JSON records, but is unsafe for other formats that
  may contain newlines or any arbitrary byte sequence.

### Binary Records

The binary output encodes values (and optionally keys) with a 4 byte length, followed by the value. Any record can be safely encoded this way.

```
format=binary
format.include.keys=true
```

NOTE: It is critical that the format settings in the S3 Source match the setting of the S3 Sink exactly, otherwise keys, values, and record contents will be corrupted.

### Custom Delimiters

The default format is text, with UTF-8 newlines between records. Keys are dropped. The delimiters and inclusion of keys can be customized:

```
# this line may be omitted
format=text
# default delimiter is a newline
format.value.delimiter=\n
# UTF-8 is the default encoding
format.value.encoding=UTF-16BE
# keys will only be written if a delimiter is specified
format.key.delimiter=\t
format.key.encoding=UTF-16BE
```

NOTE: Only the delimiter you specify is encoded. The bytes of the records will be written unchanged.
 The purpose of the config is to match the delimiter to the record encoding.

Charsets Tip: If using UTF-16, specify `UTF-16BE` or `UTF-16LE` to avoid including an addition 2 byte [BOM](https://en.wikipedia.org/wiki/Byte_order_mark#UTF-16)
  for every key and value.

### Custom Format

A custom [S3RecordFormat](https://github.com/spredfast/kafka-connect-s3/blob/master/api/src/main/java/com/spredfast/kafka/connect/s3/S3RecordFormat.java)
can be specified by providing the class name:

```
format=com.mycompany.MyS3RecordFormatImpl
format.prop1=abc
```

Refer to the [S3 Formats wiki](https://github.com/spredfast/kafka-connect-s3/wiki/S3-Formats) for more information.

## Configuration

In addition to the [standard kafka-connect config options](http://kafka.apache.org/documentation.html#connectconfigs)
and [format settings](https://github.com/spredfast/kafka-connect-s3/wiki/Sink-Configurations)
 we support/require the following, in the task properties file or REST config:

| Config Key | Default | Notes |
| ---------- | ------- | ----- |
| s3.bucket | **REQUIRED** | The name of the bucket to write too. |
| s3.prefix | `""` | Prefix added to all object keys stored in bucket to "namespace" them. |
| s3.endpoint | AWS defaults per region | Mostly useful for testing. |
| s3.path_style | `false` | Force path-style access to bucket rather than subdomain. Mostly useful for tests. |
| compressed_block_size | 67108864 | How much _uncompressed_ data to write to the file before we rol to a new block/chunk. See [Block-GZIP](#user-content-block-gzip-output-format) section above. |

Note that we use the default AWS SDK credentials provider. [Refer to their docs](http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html#id1) for the options for configuring S3 credentials.

These additional configs apply to the Source connector:

| Config Key | Default | Notes |
| ---------- | ------- | ----- |
| max.poll.records | 1000 | The number of records to return in a single poll of S3 |
| s3.page.size | 100 | The number of objects we list from S3 in one request |
| max.partition.count | 200 | The maximum number of partitions a topic can have. Partitions over this number will not be processed. |
| targetTopic.${original} | none | If you want the source to send records to an different topic than the original. e.g., targetTopic.foo=bar would send messages originally in topic foo to topic bar. |
| s3.start.marker | `null` | [List-Object Marker](http://docs.aws.amazon.com/cli/latest/reference/s3api/list-objects.html#output). S3 object key or key prefix to start reading from. |

## Contributing

Pull requests welcome! If you need ideas, check the issues for [open enhancements](https://github.com/spredfast/kafka-connect-s3/issues?q=is%3Aopen+is%3Aissue+label%3Aenhancement).
