package com.spredfast.kafka.connect.s3;

import java.util.Map;
import java.util.Objects;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.PredefinedClientConfigurations;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.retry.PredefinedBackoffStrategies;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.S3ClientOptions;
import org.apache.kafka.common.config.ConfigDef;

import static com.spredfast.kafka.connect.s3.S3ConfigurationConfig.*;

public class S3 {

	public static AmazonS3 s3client(Map<String, String> props) {
		S3ConfigurationConfig config = new S3ConfigurationConfig(props);
		return newS3Client(config);
	}

	/**
	 * Creates S3 client's configuration.
	 * This method currently configures the AWS client retry policy to use full jitter.
	 * Visible for testing.
	 *
	 * @param config the S3 configuration.
	 * @return S3 client's configuration
	 */
	protected static ClientConfiguration newClientConfiguration(S3ConfigurationConfig config) {

		ClientConfiguration clientConfiguration = PredefinedClientConfigurations.defaultConfig();
		clientConfiguration.withUserAgentPrefix("Spredfast Kafka-S3 Connect / 1.0");

		return clientConfiguration;
	}


	/**
	 * Creates and configures S3 client.
	 * Visible for testing.
	 *
	 * @param config the S3 configuration.
	 * @return S3 client
	 */
	public static AmazonS3 newS3Client(S3ConfigurationConfig config) {
		ClientConfiguration clientConfiguration = newClientConfiguration(config);
		AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
			.withAccelerateModeEnabled(
				Boolean.valueOf(config.getString(WAN_MODE_CONFIG))
			)
			.withPathStyleAccessEnabled(true)
			.withClientConfiguration(clientConfiguration);

		String region = config.getString(REGION_CONFIG);
		String url = config.getString(S3_ENDPOINT_URL_CONFIG);
		if (url != "" && url != null) {
			builder = "us-east-1".equals(region)
				? builder.withRegion(Regions.US_EAST_1)
				: builder.withRegion(region);
		} else {
			builder = builder.withEndpointConfiguration(
				new AwsClientBuilder.EndpointConfiguration(url, region)
			);
		}

		return builder.build();
	}

}
