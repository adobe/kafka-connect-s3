package com.spredfast.kafka.connect.s3.source;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
	private Iterator<SourceRecord> reader;
	private int maxPoll;
	private final Map<String, String> topicMapping = new HashMap<>();
	private S3RecordFormat format;
	private Optional<Converter> keyConverter;
	private Converter valueConverter;

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
		String bucket = this.taskConfig.get("s3.bucket");
		String prefix = this.taskConfig.get("s3.prefix");

		Set<Integer> partitionNumbers = Arrays.stream(this.taskConfig.get("partitions").split(","))
			.map(Integer::parseInt)
			.collect(toSet());
		List<S3Partition> partitions = partitionNumbers
			.stream()
			.map(p -> S3Partition.from(bucket, prefix, p))
			.collect(toList());

		Map<S3Partition, S3Offset> offsets = context.offsetStorageReader()
			.offsets(partitions.stream().map(S3Partition::asMap).collect(toList()))
			.entrySet().stream().collect(toMap(
				entry -> S3Partition.from(entry.getKey()),
				entry -> S3Offset.from(entry.getValue())));

		maxPoll = Optional.ofNullable(taskConfig.get("max.poll.records"))
			.map(Integer::parseInt)
			.orElse(1000);

		AmazonS3 client = S3.s3client(taskConfig);

		S3SourceConfig config = new S3SourceConfig(
			bucket, prefix,
			Integer.parseInt(taskConfig.getOrDefault("s3.page.size", "100")),
			taskConfig.get("s3.start.marker"),
			S3FilesReader.DEFAULT_PATTERN,
			S3FilesReader.InputFilter.GUNZIP,
			partitionNumbers::contains
		);

		log.debug("{} reading from S3 with offsets {}", taskConfig.get("name"), offsets);

		reader = new S3FilesReader(config, client, offsets, format::newReader).readAll();
	}


	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		// read up to the configured poll size
		List<SourceRecord> results = new ArrayList<>(maxPoll);

		if (stopped.get()) {
			return results;
		}
		// TODO catch AWS exceptions, sleep and retry

		while (!reader.hasNext()) {
			log.debug("Blocking until new S3 files are available.");
			// sleep and block here until new files are available
			Thread.sleep(1000L); // TODO config
			readFromStoredOffsets();
		}

		for (int i = 0; reader.hasNext() && i < maxPoll; i++) {
			SourceRecord record = reader.next();
			String topic = topicMapping.computeIfAbsent(record.topic(), this::remapTopic);
			// we know the reader returned bytes so, we can cast the key+value and use a converter to
			// generate the "real" source record
			Optional<SchemaAndValue> key = keyConverter.map(c -> c.toConnectData(topic, (byte[]) record.key()));
			SchemaAndValue value = valueConverter.toConnectData(topic, (byte[]) record.value());
			results.add(new SourceRecord(record.sourcePartition(), record.sourceOffset(), topic,
				record.kafkaPartition(),
				key.map(SchemaAndValue::schema).orElse(null), key.map(SchemaAndValue::value).orElse(null),
				value.schema(), value.value()));
		}

		log.debug("{} returning {} records.", taskConfig.get("name"), results.size());
		return results;
	}

	private String remapTopic(String originalTopic) {
		return taskConfig.getOrDefault(CONFIG_TARGET_TOPIC + "." + originalTopic, originalTopic);
	}

	@Override
	public void stop() {
		this.stopped.set(true);
	}

}
