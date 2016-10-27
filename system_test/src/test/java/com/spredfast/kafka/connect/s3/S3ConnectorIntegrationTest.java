package com.spredfast.kafka.connect.s3;

import static com.spredfast.kafka.test.KafkaIntegrationTests.givenKafkaConnect;
import static com.spredfast.kafka.test.KafkaIntegrationTests.givenLocalKafka;
import static com.spredfast.kafka.test.KafkaIntegrationTests.waitForPassing;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spredfast.kafka.connect.s3.sink.S3SinkConnector;
import com.spredfast.kafka.connect.s3.source.S3SourceConnector;
import com.spredfast.kafka.connect.s3.source.S3SourceTask;
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
		tryClose(() -> connect.close());
		tryClose(() -> kafka.close());
		tryClose(() -> s3.close(dockerClient));
		tryClose(() -> dockerClient.close());
	}

	private static void tryClose(AutoCloseable doClose) {
		try {
			doClose.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void binaryWithKeys() throws Exception {
		// this test can be flakey because we can't control how fast connect writes...
		// it's still cheaper to retry it for 3 minutes than to manually rerun builds
		waitForPassing(Duration.ofMinutes(3), () -> {
			_binaryWithKeys();
			return null;
		});
	}

	public void _binaryWithKeys() throws Exception {
		String sinkTopic = kafka.createUniqueTopic("sink-source-", 2);

		Producer<String, String> producer = givenKafkaProducer();

		givenRecords(sinkTopic, producer);

		Map<String, String> config = givenSinkConfig(sinkTopic);
		AmazonS3 s3 = givenS3Client(config);

		whenTheConnectorIsStarted(config);

		thenFilesAreWrittenToS3(s3);

		String sourceTopic = kafka.createUniqueTopic("source-replay-", 2);

		Map<String, String> sourceConfig = givenSourceConfig(sourceTopic, sinkTopic);
		whenTheConnectorIsStarted(sourceConfig);

		thenMessagesAreRestored(sourceTopic);

		whenConnectIsStopped();
		givenMoreData(sinkTopic, producer);

		whenConnectIsRestarted();
		whenTheConnectorIsStarted(config);
		whenTheConnectorIsStarted(sourceConfig);


		thenMoreMessagesAreRestored(sourceTopic);
	}

	private void whenConnectIsRestarted() throws IOException {
		connect = givenKafkaConnect(kafka.localPort());
	}

	public void givenMoreData(String sinkTopic, Producer<String, String> producer) throws InterruptedException, ExecutionException, TimeoutException {
		givenRecords(sinkTopic, producer, 5);
	}

	private void whenConnectIsStopped() throws Exception {
		connect.close();
	}


	private Consumer<String, String> givenAConsumer() {
		return new KafkaConsumer<>(ImmutableMap.of("bootstrap.servers", "localhost:" + kafka.localPort()),
			new StringDeserializer(), new StringDeserializer());
	}


	private AmazonS3 givenS3Client(Map<String, String> config) {
		AmazonS3 s3 = S3.s3client(config);
		s3.createBucket(S3_BUCKET);
		return s3;
	}

	private void whenTheConnectorIsStarted(Map<String, String> config) {
		connect.herder().putConnectorConfig(config.get("name"),
			config, false, (e, s) -> {});
	}

	private void thenFilesAreWrittenToS3(AmazonS3 s3) {
		waitForPassing(Duration.ofSeconds(10), () -> {
			List<String> keys = s3.listObjects(S3_BUCKET, S3_PREFIX).getObjectSummaries().stream()
				.map(S3ObjectSummary::getKey).collect(toList());
			List<Integer> obs = keys.stream()
				.filter(key -> key.endsWith(".gz"))
				.map(key -> key.replaceAll(".*?-(\\d{5})-\\d{12}\\.gz", "$1"))
				.map(Integer::parseInt)
				.collect(toList());
			assertEquals("should be a file for each partition " + keys, ImmutableList.of(0, 1), obs);
		});
	}


	private Map<String, String> givenSourceConfig(String sourceTopic, String sinkTopic) throws IOException {
		return s3Config(ImmutableMap.<String, String>builder()
			.put("name", sourceTopic + "-s3-source")
			.put("connector.class", S3SourceConnector.class.getName())
			.put("tasks.max", "1")
			.put(S3SourceTask.CONFIG_TARGET_TOPIC + "." + sinkTopic, sourceTopic))
			.build();
	}

	private ImmutableMap.Builder<String, String> s3Config(ImmutableMap.Builder<String, String> builder) {
		return builder
			.put("format", ByteLengthFormat.class.getName())
			.put("format.include.keys", "true")
			.put("key.converter", AlreadyBytesConverter.class.getName())

			.put("s3.bucket", S3_BUCKET)
			.put("s3.prefix", S3_PREFIX)
			.put("s3.endpoint", s3.getEndpoint())
			.put("s3.path_style", "true");  // necessary for FakeS3
	}

	private Map<String, String> givenSinkConfig(String sinkTopic) throws IOException {
		File tempDir = Files.createTempDir();
		// start sink connector
		return s3Config(ImmutableMap.<String, String>builder()
			.put("name", sinkTopic + "-s3-sink")
			.put("connector.class", S3SinkConnector.class.getName())
			.put("tasks.max", "1")
			.put("topics", sinkTopic)
			.put("local.buffer.dir", tempDir.getCanonicalPath()))
			.build();
	}

	private void givenRecords(String originalTopic, Producer<String, String> producer) throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
		givenRecords(originalTopic, producer, 0);
	}

	private void givenRecords(String originalTopic, Producer<String, String> producer, int start) throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
		// send odds to partition 1, and evens to 0
		IntStream.range(start, start + 4).forEach(i -> {
			try {
				producer.send(new ProducerRecord<>(originalTopic, i % 2, "key:" + i, "value:" + i)).get(5, TimeUnit.SECONDS);
			} catch (Exception e) {
				throw Throwables.propagate(e);
			}
		});
	}

	private void thenMessagesAreRestored(String sourceTopic) {
		thenMessagesAreRestored(sourceTopic, 0);
	}

	private void thenMessagesAreRestored(String sourceTopic, int start) {
		Consumer<String, String> consumer = givenAConsumer();
		ImmutableList<TopicPartition> partitions = ImmutableList.of(
			new TopicPartition(sourceTopic, 0),
			new TopicPartition(sourceTopic, 1)
		);
		consumer.assign(partitions);
		consumer.seek(partitions.get(0), 0);
		consumer.seek(partitions.get(1), 0);

		// HACK - add to this list and then assert we are done. will retry until done.
		List<List<String>> results = ImmutableList.of(
			new ArrayList<>(),
			new ArrayList<>()
		);
		waitForPassing(Duration.ofSeconds(10), () -> {
			ConsumerRecords<String, String> records = consumer.poll(500L);
			records.forEach(r -> results.get(r.partition()).add(r.value()));

			assertEquals("got all the records for 0 " + results, 2, distinctAfter(start, results.get(0)).count());
			assertEquals("got all the records for 1 " + results, 2, distinctAfter(start, results.get(1)).count());
		});

		boolean startOdd = (start % 2 == 1);
		List<String> evens = distinctAfter(start, results.get(0)).collect(toList());
		List<String> odds = distinctAfter(start, results.get(1)).collect(toList());
		assertEquals("records restored to same partitions, in-order", ImmutableList.of(
			ImmutableList.of("value:" + (0 + start), "value:" + (2 + start)),
			ImmutableList.of("value:" + (1 + start), "value:" + (3 + start))
		), ImmutableList.of(
			startOdd ? odds : evens,
			startOdd ? evens : odds
		));

		consumer.close();
	}

	private Stream<String> distinctAfter(int start, List<String> list) {
		return list.stream().distinct().skip(start / 2);
	}

	private void thenMoreMessagesAreRestored(String sourceTopic) {
		thenMessagesAreRestored(sourceTopic, 5);
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
