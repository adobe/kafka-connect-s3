package com.spredfast.kafka.connect.s3;

import java.util.Map;
import java.util.Objects;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.S3ClientOptions;

public class S3 {

	public static AmazonS3 s3client(Map<String, String> config) {
		// Use default credentials provider that looks in Env + Java properties + profile + instance role
		AmazonS3ClientBuilder s3Client = AmazonS3ClientBuilder.standard();

		// If worker config sets explicit endpoint override (e.g. for testing) use that
		String s3Endpoint = config.get("s3.endpoint");
		String s3Region = config.get("s3.region");
		if ((s3Endpoint != null && !Objects.equals(s3Endpoint, "")) && s3Region != null && !Objects.equals(s3Region, "")) {
			s3Client.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(s3Endpoint, s3Region));
		}
		Boolean s3PathStyle = Boolean.parseBoolean(config.get("s3.path_style"));
		if (s3PathStyle) {
			s3Client.setPathStyleAccessEnabled(true);

		}
		return s3Client.build();
	}

}
