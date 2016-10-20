package com.spredfast.kafka.connect.s3;

import static com.spredfast.kafka.test.KafkaIntegrationTests.givenKafkaConnect;
import static com.spredfast.kafka.test.KafkaIntegrationTests.givenLocalKafka;
import static com.spredfast.kafka.test.KafkaIntegrationTests.waitForPassing;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.netflix.curator.test.InstanceSpec;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.PortBinding;
import com.spredfast.kafka.connect.s3.sink.S3SinkConnector;
import com.spredfast.kafka.test.KafkaIntegrationTests;

public class S3ConnectorIntegrationTest {

	private static final String S3_BUCKET = "connect-system-test";
	private static final String S3_PREFIX = "binsystest";
	private static KafkaIntegrationTests.Kafka kafka;
	private static KafkaIntegrationTests.KafkaConnect connect;
	private static FakeS3 s3;
	private static DefaultDockerClient dockerClient;

	@BeforeClass
	public static void startKafka() throws Exception {
		kafka = givenLocalKafka();
		connect = givenKafkaConnect(kafka.localPort());
		dockerClient = givenDockerClient();

		s3 = FakeS3.create(dockerClient);

		s3.start(dockerClient);
	}

	private static DefaultDockerClient givenDockerClient() throws DockerCertificateException {
		return DefaultDockerClient.fromEnv().build();
	}

	@AfterClass
	public static void stopKafka() throws Exception {
		connect.close();
		kafka.close();
		s3.close(dockerClient);
		dockerClient.close();
	}

	@Test
	public void sourceReadsSinkWritten() throws Exception {
		String sinkTopic = kafka.createUniqueTopic("sink-source-", 2);

		Producer<String, String> producer = givenKafkaProducer();

		givenRecords(sinkTopic, producer);

		Map<String, String> config = givenSinkConfig(sinkTopic);
		AmazonS3 s3 = givenS3Client(config);

		whenTheSinkIsStarted(config);

		thenFilesAreWrittenToS3(s3);

		// TODO
		// create replay topic
		// start source connector

		// verify replay matches exactly what was written

		// stop connectors
		// write more data

		// start connectors

		// verify again

	}


	private AmazonS3 givenS3Client(Map<String, String> config) {
		AmazonS3 s3 = S3.s3client(config);
		s3.createBucket(S3_BUCKET);
		return s3;
	}

	private void whenTheSinkIsStarted(Map<String, String> config) {
		connect.herder().putConnectorConfig(config.get("name"),
			config, false, (e, s) -> {});
	}

	private void thenFilesAreWrittenToS3(AmazonS3 s3) {
		waitForPassing(Duration.ofSeconds(10), () -> {
			List<Integer> obs = s3.listObjects(S3_BUCKET, S3_PREFIX).getObjectSummaries().stream()
				.map(S3ObjectSummary::getKey)
				.filter(key -> key.endsWith(".gz"))
				.map(key -> key.replaceAll(".*?-(\\d{5})-\\d{12}\\.gz", "$1"))
				.map(Integer::parseInt)
				.collect(toList());
			assertEquals("should be a file for each partition", ImmutableList.of(0, 1), obs);
		});
	}

	private Map<String, String> givenSinkConfig(String sinkTopic) throws IOException {
		File tempDir = Files.createTempDir();
		// start sink connector
		return ImmutableMap.<String, String>builder()
			.put("name", sinkTopic + "-s3-sink")
			.put("connector.class", S3SinkConnector.class.getName())
			.put("tasks.max", "1")
			.put("topics", sinkTopic)
			.put("s3.bucket", S3_BUCKET)
			.put("s3.prefix", S3_PREFIX)
			.put("local.buffer.dir", tempDir.getCanonicalPath())
			.put("key.converter", ByteLengthEncodedConverter.class.getName())
			.put("value.converter", ByteLengthEncodedConverter.class.getName())
			.put("s3.endpoint", s3.getEndpoint())
			.put("s3.path_style", "true") // necessary for FakeS3
			.build();
	}

	public static class FakeS3 {
		private static final String IMAGE = "lphoward/fake-s3";
		private static final String CONTAINER_PORT = "4569";
		private final int hostPort;
		private final ContainerCreation container;


		private FakeS3(int hostPort, ContainerCreation container) {
			this.hostPort = hostPort;
			this.container = container;
		}

		public static FakeS3 create(DockerClient dockerClient) throws DockerException, InterruptedException {
			// make sure we have the image
			dockerClient.pull(IMAGE, System.err::println);

			// bind a fakes3 image to a random host port
			int port = InstanceSpec.getRandomPort();
			return new FakeS3(port, dockerClient.createContainer(ContainerConfig.builder()
				.hostConfig(HostConfig.builder().portBindings(ImmutableMap.of(
					CONTAINER_PORT, ImmutableList.of(PortBinding.of("0.0.0.0", port))
				)).build())
				.image(IMAGE)
				.exposedPorts(ImmutableSet.of(CONTAINER_PORT))
				.build()));
		}

		public void start(DockerClient dockerClient) throws DockerException, InterruptedException {
			dockerClient.startContainer(container.id());
		}

		public void close(DockerClient dockerClient) throws DockerException, InterruptedException {
			dockerClient.stopContainer(container.id(), 10);
			dockerClient.removeContainer(container.id());
		}

		public String getEndpoint() {
			return "http://localhost:" + hostPort;
		}
	}

	private void givenRecords(String originalTopic, Producer<String, String> producer) throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
		// send odds to partition 1, and evens to 0
		producer.send(new ProducerRecord<>(originalTopic, 0, "key:0", "value:0")).get(5, TimeUnit.SECONDS);
		producer.send(new ProducerRecord<>(originalTopic, 1, "key:1", "value:1")).get(5, TimeUnit.SECONDS);
		producer.send(new ProducerRecord<>(originalTopic, 0, "key:2", "value:2")).get(5, TimeUnit.SECONDS);
		producer.send(new ProducerRecord<>(originalTopic, 1, "key:3", "value:3")).get(5, TimeUnit.SECONDS);
	}

	private Producer<String, String> givenKafkaProducer() {
		return new KafkaProducer<>(ImmutableMap.<String, Object>builder()
			.put("bootstrap.servers", "localhost:" + kafka.localPort())
			.put("key.serializer", StringSerializer.class.getName())
			.put("value.serializer", StringSerializer.class.getName())
			.put("retries", "5")
			.put("acks", "all")
			.build());
	}
}
