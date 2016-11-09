package com.spredfast.kafka.connect.s3.source;

public class S3SourceRecord {
	private final S3Partition file;
	private final S3Offset offset;
	private final String topic;
	private final int partition;
	private final byte[] key;
	private final byte[] value;


	public S3SourceRecord(S3Partition file, S3Offset offset, String topic, int partition, byte[] key, byte[] value) {
		this.file = file;
		this.offset = offset;
		this.topic = topic;
		this.partition = partition;
		this.key = key;
		this.value = value;
	}

	public S3Partition file() {
		return file;
	}

	public S3Offset offset() {
		return offset;
	}

	public String topic() {
		return topic;
	}

	public int partition() {
		return partition;
	}

	public byte[] key() {
		return key;
	}

	public byte[] value() {
		return value;
	}
}
