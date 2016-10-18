package com.spredfast.kafka.connect.s3.source;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class S3Partition {
	private final String bucket;
	private final String keyPrefix;
	private final int partition;

	public S3Partition(String bucket, String keyPrefix, int partition) {
		this.bucket = bucket;
		this.keyPrefix = normalizePrefix(keyPrefix);
		this.partition = partition;
	}

	public static S3Partition from(String bucket, String keyPrefix, int partition) {
		return new S3Partition(bucket, keyPrefix, partition);
	}

	public static S3Partition from(Map<String, Object> map) {
		String bucket = (String) map.get("bucket");
		String keyPrefix = (String) map.get("keyPrefix");
		int partition = ((Number) map.get("kafkaPartition")).intValue();
		return new S3Partition(bucket, keyPrefix, partition);
	}

	public static String normalizePrefix(String keyPrefix) {
		return keyPrefix == null ? ""
			: keyPrefix.endsWith("/") ? keyPrefix : keyPrefix + "/";
	}

	public Map<String, Object> asMap() {
		Map<String, Object> map = new HashMap<>();
		map.put("bucket", bucket);
		map.put("keyPrefix", keyPrefix);
		map.put("kafkaPartition", partition);
		return map;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		S3Partition that = (S3Partition) o;
		return partition == that.partition &&
			Objects.equals(bucket, that.bucket) &&
			Objects.equals(keyPrefix, that.keyPrefix);
	}

	@Override
	public int hashCode() {
		return Objects.hash(bucket, keyPrefix, partition);
	}

	@Override
	public String toString() {
		return bucket + "/" + keyPrefix + "partition:" + partition;
	}
}
