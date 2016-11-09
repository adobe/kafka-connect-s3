package com.spredfast.kafka.connect.s3.source;

import java.util.HashMap;
import java.util.Map;

public class S3Offset implements Comparable<S3Offset> {

	private final String s3key;

	private final long offset;

	public S3Offset(String s3key, long offset) {
		this.s3key = s3key;
		this.offset = offset;
	}

	public static S3Offset from(String s3key, long offset) {
		return new S3Offset(s3key, offset);
	}

	public static S3Offset from(Map<String, Object> map) {
		return from((String) map.get("s3key"), (Long) map.get("originalOffset"));
	}

	public String getS3key() {
		return s3key;
	}

	public long getOffset() {
		return offset;
	}

	public Map<String, ?> asMap() {
		Map<String, Object> map = new HashMap<>();
		map.put("s3key", s3key);
		map.put("originalOffset", offset);
		return map;
	}

	@Override
	public String toString() {
		return s3key + "@" + offset;
	}

	@Override
	public int compareTo(S3Offset o) {
		int i = s3key.compareTo(o.s3key);
		return i == 0 ? (int) (offset - o.offset) : i;
	}
}
