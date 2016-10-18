package com.spredfast.kafka.connect.s3;

import java.util.Map;
import java.util.Objects;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;

public class S3 {

	public static AmazonS3 s3client(Map<String, String> config) {
		// Use default credentials provider that looks in Env + Java properties + profile + instance role
		AmazonS3 s3Client = new AmazonS3Client();

		// If worker config sets explicit endpoint override (e.g. for testing) use that
		String s3Endpoint = config.get("s3.endpoint");
		if (s3Endpoint != null && !Objects.equals(s3Endpoint, "")) {
			s3Client.setEndpoint(s3Endpoint);
		}
		Boolean s3PathStyle = Boolean.parseBoolean(config.get("s3.path_style"));
		if (s3PathStyle) {
			s3Client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
		}
		return s3Client;
	}

}
