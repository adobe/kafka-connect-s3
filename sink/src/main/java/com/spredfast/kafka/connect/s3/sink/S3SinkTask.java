package com.spredfast.kafka.connect.s3.sink;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.services.s3.AmazonS3;
import com.spredfast.kafka.connect.s3.AlreadyBytesConverter;
import com.spredfast.kafka.connect.s3.Configure;
import com.spredfast.kafka.connect.s3.Constants;
import com.spredfast.kafka.connect.s3.Metrics;
import com.spredfast.kafka.connect.s3.S3;
import com.spredfast.kafka.connect.s3.S3RecordFormat;
import com.spredfast.kafka.connect.s3.S3RecordsWriter;


public class S3SinkTask extends SinkTask {

	private static final Logger log = LoggerFactory.getLogger(S3SinkTask.class);

	private Map<String, String> config;

	private final Map<TopicPartition, PartitionWriter> partitions = new LinkedHashMap<>();

	private long GZIPChunkThreshold = 67108864;

	private S3Writer s3;

	private Optional<Converter> keyConverter;

	private Converter valueConverter;

	private S3RecordFormat recordFormat;

	private Metrics metrics;

	private Map<String, String> tags;

	@Override
	public String version() {
		return Constants.VERSION;
	}

	@Override
	public void start(Map<String, String> props) throws ConnectException {
		config = new HashMap<>(props);

		configGet("compressed_block_size").map(Long::parseLong).ifPresent(chunkThreshold ->
			this.GZIPChunkThreshold = chunkThreshold);

		recordFormat = Configure.createFormat(props);

		keyConverter = ofNullable(Configure.buildConverter(config, "key.converter", true, null));
		valueConverter = Configure.buildConverter(config, "value.converter", false, AlreadyBytesConverter.class);

		String bucket = configGet("s3.bucket")
			.filter(s -> !s.isEmpty())
			.orElseThrow(() -> new ConnectException("S3 bucket must be configured"));
		String prefix = configGet("s3.prefix")
			.orElse("");
		AmazonS3 s3Client = S3.s3client(config);

		s3 = new S3Writer(bucket, prefix, s3Client);

		metrics = Configure.metrics(props);
		tags = Configure.parseTags(props.get("metrics.tags"));
		tags.put("connector_name", name());

		// Recover initial assignments
		open(context.assignment());
	}

	private Optional<String> configGet(String key) {
		return ofNullable(config.get(key));
	}

	@Override
	public void stop() throws ConnectException {
		// ensure we delete our temp files
		for (PartitionWriter writer : partitions.values()) {
			log.debug("{} Stopping - Deleting temp file {}", name(), writer.getDataFilePath());
			writer.delete();
		}
	}

	@Override
	public void put(Collection<SinkRecord> records) throws ConnectException {
		records.stream().collect(groupingBy(record -> new TopicPartition(record.topic(), record.kafkaPartition()))).forEach((tp, rs) -> {
			long firstOffset = rs.get(0).kafkaOffset();
			long lastOffset = rs.get(rs.size() - 1).kafkaOffset();

			log.debug("{} received {} records for {} to archive. Last offset {}", name(), rs.size(), tp,
				lastOffset);

			PartitionWriter writer = partitions.computeIfAbsent(tp,
				t -> initWriter(t, firstOffset));
			writer.writeAll(rs);
		});
	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) throws ConnectException {
		Metrics.StopTimer timer = metrics.time("flush", tags);

		log.debug("{} flushing offsets", name());

		// XXX the docs for flush say that the offsets given are the same as if we tracked the offsets
		// of the records given to put, so we should just write whatever we have in our files
		offsets.keySet().stream()
			.map(partitions::get)
			.filter(p -> p != null) // TODO error/warn?
			.forEach(PartitionWriter::done);

		timer.stop();
	}

	private String name() {
		return configGet("name").orElseThrow(() -> new IllegalWorkerStateException("Tasks always have names"));
	}

	@Override
	public void close(Collection<TopicPartition> partitions) {
		// have already flushed, so just ensure the temp files are deleted (in case flush threw an exception)
		partitions.stream()
			.map(this.partitions::get)
			.filter(p -> p != null)
			.forEach(PartitionWriter::delete);
	}

	@Override
	public void open(Collection<TopicPartition> partitions) {
		// nothing to do. we will create files when we are given the first record for a partition
		// offsets are managed by Connect
	}

	private PartitionWriter initWriter(TopicPartition tp, long offset) {
		try {
			return new PartitionWriter(tp, offset);
		} catch (IOException e) {
			throw new RetriableException("Error initializing writer for " + tp + " at offset " + offset, e);
		}
	}

	private class PartitionWriter {
		private final TopicPartition tp;
		private final BlockGZIPFileWriter writer;
		private final S3RecordsWriter format;
		private final Map<String, String> tags;
		private boolean finished;
		private boolean closed;

		private PartitionWriter(TopicPartition tp, long firstOffset) throws IOException {
			this.tp = tp;
			format = recordFormat.newWriter();

			String name = String.format("%s-%05d", tp.topic(), tp.partition());
			String path = configGet("local.buffer.dir")
				.orElseThrow(() -> new ConnectException("No local buffer file path configured"));
			log.debug("New temp file: {}/{} @ {}", path, name, firstOffset);

			Map<String, String> writerTags = new HashMap<>(S3SinkTask.this.tags);
			writerTags.put("kafka_topic", tp.topic());
			writerTags.put("kafka_partition", "" + tp.partition());
			this.tags = writerTags;

			writer = new BlockGZIPFileWriter(name, path, firstOffset, GZIPChunkThreshold, format.init(tp.topic(), tp.partition(), firstOffset));
		}

		private void writeAll(Collection<SinkRecord> records) {
			metrics.hist(records.size(), "putSize", tags);
			try (Metrics.StopTimer ignored = metrics.time("writeAll", tags)) {
				writer.write(format.writeBatch(records.stream().map(record -> new ProducerRecord<>(record.topic(), record.kafkaPartition(),
					keyConverter.map(c -> c.fromConnectData(record.topic(), record.keySchema(), record.key()))
						.orElse(null),
					valueConverter.fromConnectData(record.topic(), record.valueSchema(), record.value())
				))).collect(toList()), records.size());
			} catch (IOException e) {
				throw new RetriableException("Failed to write to buffer", e);
			}
		}

		public String getDataFilePath() {
			return writer.getDataFilePath();
		}

		public void delete() {
			writer.delete();
			partitions.remove(tp);
		}

		public void done() {
			Metrics.StopTimer time = metrics.time("s3Put", tags);
			try {
				if (!finished) {
					writer.write(Arrays.asList(format.finish(tp.topic(), tp.partition())), 0);
					finished = true;
				}
				if (!closed) {
					writer.close();
					closed = true;
				}
				s3.putChunk(writer.getDataFilePath(), writer.getIndexFilePath(), tp);
			} catch (IOException e) {
				throw new RetriableException("Error flushing " + tp, e);
			}

			delete();
			time.stop();
		}

	}
}
