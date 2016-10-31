package com.spredfast.kafka.connect.s3.source;

import java.util.regex.Pattern;

public class S3SourceConfig {
	public final String bucket;
	public String keyPrefix = "";
	public int pageSize = 500;
	public String startMarker = null; // for partial replay
	public Pattern keyPattern = S3FilesReader.DEFAULT_PATTERN;
	public S3FilesReader.InputFilter inputFilter = S3FilesReader.InputFilter.GUNZIP;
	public S3FilesReader.PartitionFilter partitionFilter = S3FilesReader.PartitionFilter.MATCH_ALL;

	public S3SourceConfig(String bucket) {
		this.bucket = bucket;
	}

	public S3SourceConfig(String bucket, String keyPrefix, int pageSize, String startMarker, Pattern keyPattern, S3FilesReader.InputFilter inputFilter, S3FilesReader.PartitionFilter partitionFilter) {
		this.bucket = bucket;
		this.keyPrefix = keyPrefix;
		this.pageSize = pageSize;
		this.startMarker = startMarker;
		this.keyPattern = keyPattern;
		if (inputFilter != null) {
			this.inputFilter = inputFilter;
		}
		if (partitionFilter != null) {
			this.partitionFilter = partitionFilter;
		}
	}
}
