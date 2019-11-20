package com.spredfast.kafka.connect.s3;

import static com.google.common.collect.Maps.immutableEntry;
import static com.google.common.collect.Maps.transformValues;
import static com.spredfast.kafka.test.KafkaIntegrationTests.givenKafkaConnect;
import static com.spredfast.kafka.test.KafkaIntegrationTests.givenLocalKafka;
import static com.spredfast.kafka.test.KafkaIntegrationTests.waitForPassing;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.minBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spredfast.kafka.connect.s3.json.ChunksIndex;
import com.spredfast.kafka.connect.s3.sink.S3SinkConnector;
import com.spredfast.kafka.connect.s3.sink.S3SinkTask;
import com.spredfast.kafka.connect.s3.source.S3FilesReader;
import com.spredfast.kafka.connect.s3.source.S3SourceConnector;
import com.spredfast.kafka.connect.s3.source.S3SourceTask;
import com.spredfast.kafka.test.KafkaIntegrationTests;

@Ignore("Doesn't seem to work on CircleCI 2.0 - I believe it has to do with the Docker Client not being compatible")
public class S3ConnectorIntegrationTest {

	private static final String S3_BUCKET = "connect-system-test";
	private static final String S3_PREFIX = "binsystest";
	private static KafkaIntegrationTests.Kafka kafka;
	private static KafkaIntegrationTests.KafkaConnect connect;
	private static FakeS3 s3;
	private static DefaultDockerClient dockerClient;
	private static KafkaIntegrationTests.KafkaConnect stringConnect;

	@BeforeClass
	public static void startKafka() throws Exception {
		kafka = givenLocalKafka();
		connect = givenKafkaConnect(kafka.localPort());
		stringConnect = givenKafkaConnect(kafka.localPort(), ImmutableMap.of(
			"value.converter", StringConverter.class.getName()
		));
		dockerClient = givenDockerClient();

		s3 = FakeS3.create(dockerClient);

		s3.start(dockerClient);

		ConsoleAppender consoleAppender = new ConsoleAppender(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN));
		Logger.getRootLogger().addAppender(consoleAppender);
		Logger.getRootLogger().setLevel(Level.ERROR);
		Logger.getLogger(S3SinkTask.class).setLevel(Level.DEBUG);
		Logger.getLogger(S3SourceTask.class).setLevel(Level.DEBUG);
		Logger.getLogger(S3FilesReader.class).setLevel(Level.DEBUG);
		Logger.getLogger(FileOffsetBackingStore.class).setLevel(Level.DEBUG);
	}

	private static DefaultDockerClient givenDockerClient() throws DockerCertificateException {
		return DefaultDockerClient.fromEnv().build();
	}


	@AfterClass
	public static void stopKafka() throws Exception {
		tryClose(connect);
		tryClose(stringConnect);
		tryClose(kafka);
		tryClose(() -> s3.close(dockerClient));
		tryClose(dockerClient);
	}

	private static void tryClose(AutoCloseable doClose) {
		try {
			doClose.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@After
	public void cleanup() {
		// stop all the connectors
		connect.herder().connectors((e, connectors) ->
			connectors.forEach(this::whenTheSinkIsStopped));
	}


	private void whenTheSinkIsStopped(String name) {
		connect.herder().putConnectorConfig(name, null, true,
			(e, c) -> { if (e != null) Throwables.propagate(e); });
	}

	// this seems to take at least 20 seconds to get a segment deleted :(
	private Map.Entry<String, List<Long>> slow_givenATopicWithNonZeroStartOffsets() throws InterruptedException, ExecutionException, TimeoutException {
		Properties props = new Properties();
		props.put("segment.bytes", "100");
		props.put("segment.ms", "500");
		props.put("retention.ms", "500");
		String topic = kafka.createUniqueTopic("non-zero-", 2, props);
		Producer<String, String> producer = givenKafkaProducer();

		// produce a bunch of records
		for(int i = 0; i < 200; i++) {
			producer.send(new ProducerRecord<>(topic, i % 2, "skip", "ignore")).get(5, TimeUnit.SECONDS);
		}
		Consumer<String, String> consumer = givenAConsumer();
		consumer.assign(ImmutableList.of(new TopicPartition(topic, 0), new TopicPartition(topic, 1)));
		ImmutableList<Long> offsets = waitForPassing(Duration.ofMinutes(10), () -> {
			consumer.seekToBeginning(ImmutableList.of(new TopicPartition(topic, 0), new TopicPartition(topic, 1)));

			long zero = consumer.position(new TopicPartition(topic, 0));
			long one = consumer.position(new TopicPartition(topic, 1));
			assertNotEquals("partition 0: 0 has rolled out", 0L, zero);
			assertNotEquals("partition 1: 0 has rolled out", 0L, one);

			return ImmutableList.of(zero, one);
		});
		consumer.close();
		// revert the retention to something reasonable
		props.put("retention.ms", "300000");
		kafka.updateTopic(topic, props);
		return immutableEntry(topic, offsets);
	}

	@Test
	public void binaryWithKeys() throws Exception {
		// this gross, expensive initialization is necessary to ensure we are handling offsets correctly
		// If we're not, the restart will produce duplicates in S3, and files with incorrect offsets.
		Map.Entry<String, List<Long>> topicAndOffsets = slow_givenATopicWithNonZeroStartOffsets();
		String sinkTopic = topicAndOffsets.getKey();

		Producer<String, String> producer = givenKafkaProducer();

		givenRecords(sinkTopic, producer);

		Map<String, String> sinkConfig = givenSinkConfig(sinkTopic);
		AmazonS3 s3 = givenS3Client(sinkConfig);

		whenTheConnectorIsStarted(sinkConfig);

		thenFilesAreWrittenToS3(s3, sinkTopic, topicAndOffsets.getValue());

		String sourceTopic = kafka.createUniqueTopic("bin-source-replay-", 2);

		Map<String, String> sourceConfig = givenSourceConfig(sourceTopic, sinkTopic);
		whenTheConnectorIsStarted(sourceConfig);

		thenMessagesAreRestored(sourceTopic, s3);

		whenConnectIsStopped();
		givenMoreData(sinkTopic, producer);

		whenConnectIsRestarted();
		whenTheConnectorIsStarted(sinkConfig);
		whenTheConnectorIsStarted(sourceConfig);

		// This will fail if we get duplicates into the sink topic, which can happen because we have duplicates
		// in S3 (source bug) or because of a bug in the source.
		thenMoreMessagesAreRestored(sourceTopic, s3);

		whenTheSinkIsStopped(sinkConfig.get("name"));

		thenTempFilesAreCleanedUp(sinkConfig);
	}


	private void thenTempFilesAreCleanedUp(Map<String, String> sinkConfig) {
		//noinspection ConstantConditions
		waitForPassing(Duration.ofSeconds(3), () ->
			assertEquals(ImmutableList.of(), ImmutableList.copyOf(new File(sinkConfig.get("local.buffer.dir")).list())));
	}

	@Test
	public void stringWithoutKeys() throws Exception {
		String sinkTopic = kafka.createUniqueTopic("txt-sink-source-", 2);

		Producer<String, String> producer = givenKafkaProducer();

		givenRecords(sinkTopic, producer);

		Map<String, String> sinkConfig = givenStringValues(givenSinkConfig(sinkTopic));
		whenTheConnectorIsStarted(sinkConfig, stringConnect);

		String sourceTopic = kafka.createUniqueTopic("txt-source-replay-", 2);

		Map<String, String> sourceConfig = givenStringValues(givenSourceConfig(sourceTopic, sinkTopic));
		whenTheConnectorIsStarted(sourceConfig, stringConnect);

		thenMessagesAreRestored(sourceTopic, givenS3Client(sinkConfig));
	}

	private Map<String, String> givenStringValues(Map<String, String> config) {
		Map<String, String> copy = new HashMap<>(config);
		copy.remove("key.converter");
		copy.remove("format.include.keys");
		copy.remove("format");
		copy.putAll(ImmutableMap.of(
			"value.converter", StringConverter.class.getName()
		));
		return copy;
	}

	private void whenConnectIsRestarted() throws IOException {
		connect = connect.restart();
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
		whenTheConnectorIsStarted(config, connect);
	}

	private void whenTheConnectorIsStarted(Map<String, String> config, KafkaIntegrationTests.KafkaConnect connect) {
		connect.herder().putConnectorConfig(config.get("name"),
			config, false, (e, s) -> {});
	}

	private void thenFilesAreWrittenToS3(AmazonS3 s3, String sinkTopic, List<Long> offsets) {
		waitForPassing(Duration.ofSeconds(10), () -> {
			List<String> keys = s3.listObjects(S3_BUCKET, S3_PREFIX).getObjectSummaries().stream()
				.map(S3ObjectSummary::getKey).collect(toList());
			Set<Map.Entry<Integer, Long>> partAndOffset = keys.stream()
				.filter(key -> key.endsWith(".gz") && key.contains(sinkTopic))
				.map(key -> immutableEntry(Integer.parseInt(key.replaceAll(".*?-(\\d{5})-\\d{12}\\.gz", "$1")),
					Long.parseLong(key.replaceAll(".*?-\\d{5}-(\\d{12})\\.gz", "$1"))))
				.collect(toSet());

			Map<Integer, Long> startOffsets = transformValues(partAndOffset.stream()
					.collect(groupingBy(Map.Entry::getKey, minBy(Map.Entry.comparingByValue()))),
				(optEntry) -> optEntry.map(Map.Entry::getValue).orElse(0L));

			assertTrue(startOffsets + "[0] !~ " + offsets, ofNullable(startOffsets.get(0)).orElse(-1L) >= offsets.get(0));
			assertTrue(startOffsets + "[1] !~ " + offsets, ofNullable(startOffsets.get(1)).orElse(-1L) >= offsets.get(1));
		});

	}

	@SafeVarargs
	private final <T> void assertEqualsOneOf(String message, T actual, T... expected) {
		if (!Arrays.stream(expected).anyMatch(e -> e.equals(message))) {
			assertEquals(message, expected[0], actual);
		}
	}


	private Map<String, String> givenSourceConfig(String sourceTopic, String sinkTopic) throws IOException {
		return s3Config(ImmutableMap.<String, String>builder()
			.put("name", sourceTopic + "-s3-source")
			.put("connector.class", S3SourceConnector.class.getName())
			.put("tasks.max", "2")
			.put("topics", sinkTopic)
			.put("max.partition.count", "2")
			.put(S3SourceTask.CONFIG_TARGET_TOPIC + "." + sinkTopic, sourceTopic))
			.build();
	}

	private ImmutableMap.Builder<String, String> s3Config(ImmutableMap.Builder<String, String> builder) {
		return builder
			.put("format", ByteLengthFormat.class.getName())
			.put("format.include.keys", "true")
			.put("key.converter", AlreadyBytesConverter.class.getName())

			.put("s3.new.record.poll.interval", "200") // poll fast

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
			.put("tasks.max", "2")
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

	private void thenMessagesAreRestored(String sourceTopic, AmazonS3 s3) {
		thenMessagesAreRestored(sourceTopic, 0, s3);
	}

	private void thenMessagesAreRestored(String sourceTopic, int start, AmazonS3 s3) {
		Consumer<String, String> consumer = givenAConsumer();
		ImmutableList<TopicPartition> partitions = ImmutableList.of(
			new TopicPartition(sourceTopic, 0),
			new TopicPartition(sourceTopic, 1)
		);
		consumer.assign(partitions);
		consumer.seekToBeginning(partitions);

		// HACK - add to this list and then assert we are done. will retry until done.
		List<List<String>> results = ImmutableList.of(
			new ArrayList<>(),
			new ArrayList<>()
		);
		waitForPassing(Duration.ofSeconds(30), () -> {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500L));
			StreamSupport.stream(records.spliterator(), false)
				.filter(r -> !"skip".equals(r.key()))
				.forEach(r -> results.get(r.partition()).add(r.value()));

			assertEquals("got all the records for 0 " + results, 2, distinctAfter(start, results.get(0)).count());
			assertEquals("got all the records for 1 " + results, 2, distinctAfter(start, results.get(1)).count());
		});

		boolean startOdd = (start % 2 == 1);
		List<String> evens = results.get(0).stream().skip(start / 2).collect(toList());
		List<String> odds = results.get(1).stream().skip(start / 2).collect(toList());
		ImmutableList<ImmutableList<String>> expected = ImmutableList.of(
			ImmutableList.of("value:" + (0 + start), "value:" + (2 + start)),
			ImmutableList.of("value:" + (1 + start), "value:" + (3 + start))
		);
		ImmutableList<List<String>> actual = ImmutableList.of(
			startOdd ? odds : evens,
			startOdd ? evens : odds
		);
		if (!expected.equals(actual)) {
			// list the S3 chunks, for debugging
			String chunks = s3.listObjects(S3_BUCKET, S3_PREFIX).getObjectSummaries().stream().filter(s -> s.getKey().endsWith(".json"))
				.flatMap(idx -> {
					try {
						return Stream.concat(Stream.of("\n===" + idx.getKey() + "==="),
							((ChunksIndex) new ObjectMapper().readerFor(ChunksIndex.class).readValue(s3.getObject(S3_BUCKET, idx.getKey()).getObjectContent()))
							.chunks.stream().map(c -> String.format("[%d..%d]",
								c.first_record_offset, c.first_record_offset + c.num_records)));
					} catch (IOException e) {
						return null;
					}
				}).collect(joining("\n"));

			// NOTE: results is asserted to assist in debugging. it may contain values that should be skipped
			assertEquals("records restored to same partitions, in-order. Chunks = " + chunks, expected, results);
		}

		consumer.close();
	}

	private Stream<String> distinctAfter(int start, List<String> list) {
		return list.stream().distinct().skip(start / 2);
	}

	private void thenMoreMessagesAreRestored(String sourceTopic, AmazonS3 s3) {
		thenMessagesAreRestored(sourceTopic, 5, s3);
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
