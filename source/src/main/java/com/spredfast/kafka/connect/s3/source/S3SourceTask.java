package com.spredfast.kafka.connect.s3.source;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.spredfast.kafka.connect.s3.AlreadyBytesConverter;
import com.spredfast.kafka.connect.s3.Constants;
import com.spredfast.kafka.connect.s3.Configure;
import com.spredfast.kafka.connect.s3.S3;
import com.spredfast.kafka.connect.s3.S3RecordFormat;

public class S3SourceTask extends SourceTask {
	private static final Logger log = LoggerFactory.getLogger(S3SourceTask.class);

	/**
	 * @see #remapTopic(String)
	 */
	public static final String CONFIG_TARGET_TOPIC = "targetTopic";
	private final AtomicBoolean stopped = new AtomicBoolean();

	private Map<String, String> taskConfig;
	private Iterator<S3SourceRecord> reader;
	private int maxPoll;
	private final Map<String, String> topicMapping = new HashMap<>();
	private S3RecordFormat format;
	private Optional<Converter> keyConverter;
	private Converter valueConverter;
	private long s3PollInterval = 10_000L;
	private long errorBackoff = 1000L;
	private Map<S3Partition, S3Offset> offsets;

	@Override
	public String version() {
		return Constants.VERSION;
	}

	@Override
	public void start(Map<String, String> taskConfig) {
		this.taskConfig = taskConfig;
		format = Configure.createFormat(taskConfig);

		keyConverter = Optional.ofNullable(Configure.buildConverter(taskConfig, "key.converter", true, null));
		valueConverter = Configure.buildConverter(taskConfig, "value.converter", false, AlreadyBytesConverter.class);

		readFromStoredOffsets();
	}

	private void readFromStoredOffsets() {
		try {
			tryReadFromStoredOffsets();
		} catch (Exception e) {
			throw new ConnectException("Couldn't start task " + taskConfig, e);
		}
	}

	private void tryReadFromStoredOffsets() throws UnsupportedEncodingException {
		String bucket = configGet("s3.bucket").orElseThrow(() -> new ConnectException("No bucket configured!"));
		String prefix = configGet("s3.prefix").orElse("");

		Set<Integer> partitionNumbers = Arrays.stream(configGet("partitions").orElseThrow(() -> new IllegalStateException("no assigned parititions!?")).split(","))
			.map(Integer::parseInt)
			.collect(toSet());

		Set<String> topics = configGet("topics")
			.map(Object::toString)
			.map(s -> Arrays.stream(s.split(",")).collect(toSet()))
			.orElseGet(HashSet::new);

		List<S3Partition> partitions = partitionNumbers
			.stream()
			.flatMap(p -> topics.stream().map(t -> S3Partition.from(bucket, prefix, t, p)))
			.collect(toList());

		// need to maintain internal offset state forever. task will be committed and stopped if
		// our partitions change, so internal state should always be the most accurate
		if (offsets == null) {
			offsets = context.offsetStorageReader()
				.offsets(partitions.stream().map(S3Partition::asMap).collect(toList()))
				.entrySet().stream().filter(e -> e.getValue() != null)
				.collect(toMap(
					entry -> S3Partition.from(entry.getKey()),
					entry -> S3Offset.from(entry.getValue())));
		}

		maxPoll = configGet("max.poll.records")
			.map(Integer::parseInt)
			.orElse(1000);
		s3PollInterval = configGet("s3.new.record.poll.interval")
			.map(Long::parseLong)
			.orElse(10_000L);
		errorBackoff = configGet("s3.error.backoff")
			.map(Long::parseLong)
			.orElse(1000L);

		AmazonS3 client = S3.s3client(taskConfig);


		S3SourceConfig config = new S3SourceConfig(
			bucket, prefix,
			configGet("s3.page.size").map(Integer::parseInt).orElse(100),
			configGet("s3.start.marker").orElse(null),
			S3FilesReader.DEFAULT_PATTERN,
			S3FilesReader.InputFilter.GUNZIP,
			S3FilesReader.PartitionFilter.from((topic, partition) ->
				(topics.isEmpty() || topics.contains(topic))
				&& partitionNumbers.contains(partition))
		);

		log.debug("{} reading from S3 with offsets {}", name(), offsets);

		reader = new S3FilesReader(config, client, offsets, format::newReader).readAll();
	}

	private Optional<String> configGet(String key) {
		return Optional.ofNullable(taskConfig.get(key));
	}


	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		// read up to the configured poll size
		List<SourceRecord> results = new ArrayList<>(maxPoll);

		if (stopped.get()) {
			return results;
		}

		// AWS errors will happen. Nothing to do about it but sleep and try again.
		while(!stopped.get()) {
			try {
				return getSourceRecords(results);
			} catch (AmazonS3Exception e) {
				if (e.isRetryable()) {
					log.warn("Retryable error while polling. Will sleep and try again.", e);
					Thread.sleep(errorBackoff);
					readFromStoredOffsets();
				} else {
					// die
					throw e;
				}
			}
		}
		return results;
	}

	private List<SourceRecord> getSourceRecords(List<SourceRecord> results) throws InterruptedException {
		while (!reader.hasNext() && !stopped.get()) {
			log.debug("Blocking until new S3 files are available.");
			// sleep and block here until new files are available
			Thread.sleep(s3PollInterval);
			readFromStoredOffsets();
		}

		if (stopped.get()) {
			return results;
		}

		for (int i = 0; reader.hasNext() && i < maxPoll && !stopped.get(); i++) {
			S3SourceRecord record = reader.next();
			updateOffsets(record.file(), record.offset());
			String topic = topicMapping.computeIfAbsent(record.topic(), this::remapTopic);
			// we know the reader returned bytes so, we can cast the key+value and use a converter to
			// generate the "real" source record
			Optional<SchemaAndValue> key = keyConverter.map(c -> c.toConnectData(topic, record.key()));
			SchemaAndValue value = valueConverter.toConnectData(topic, record.value());
			results.add(new SourceRecord(record.file().asMap(), record.offset().asMap(), topic,
				record.partition(),
				key.map(SchemaAndValue::schema).orElse(null), key.map(SchemaAndValue::value).orElse(null),
				value.schema(), value.value()));
		}

		log.debug("{} returning {} records.", name(), results.size());
		return results;
	}

	private void updateOffsets(S3Partition file, S3Offset offset) {
		// store the larger offset. we don't read out of order (could probably get away with always writing what we are handed)
		S3Offset current = offsets.getOrDefault(file, offset);
		if (current.compareTo(offset) < 0) {
			log.debug("{} updated offset for {} to {}", name(), file, offset);
			offsets.put(file, offset);
		} else {
			offsets.put(file, current);
		}
	}

	@Override
	public void commit() throws InterruptedException {
		log.debug("{} Commit offsets {}", name(), offsets);
	}

	@Override
	public void commitRecord(SourceRecord record) throws InterruptedException {
		log.debug("{} Commit record w/ offset {}", name(), record.sourceOffset());
	}

	private String name() {
		return configGet("name").orElse("???");
	}

	private String remapTopic(String originalTopic) {
		return taskConfig.getOrDefault(CONFIG_TARGET_TOPIC + "." + originalTopic, originalTopic);
	}

	@Override
	public void stop() {
		this.stopped.set(true);
	}

}
