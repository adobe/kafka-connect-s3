package com.spredfast.kafka.connect.s3.source;

import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.spredfast.kafka.connect.s3.LazyString;
import com.spredfast.kafka.connect.s3.S3RecordsReader;
import com.spredfast.kafka.connect.s3.json.ChunkDescriptor;
import com.spredfast.kafka.connect.s3.json.ChunksIndex;

/**
 * Helpers for reading records out of S3. Not thread safe.
 * Records should be in order since S3 lists files in lexicographic order.
 * It is strongly recommended that you use a unique key prefix per topic as
 * there is no option to restrict this reader by topic.
 * <p>
 * NOTE: hasNext() on the returned iterators may throw AmazonClientException if there
 * was a problem communicating with S3 or reading an object. Your code should
 * catch AmazonClientException and implement back-off and retry as desired.
 * <p>
 * Any other exception should be considered a permanent failure.
 */
public class S3FilesReader implements Iterable<S3SourceRecord> {

	private static final Logger log = LoggerFactory.getLogger(S3FilesReader.class);

	public static final Pattern DEFAULT_PATTERN = Pattern.compile(
		"(\\/|^)"                        // match the / or the start of the key so we shouldn't have to worry about prefix
			+ "(?<topic>[^/]+?)-"            // assuming no / in topic names
			+ "(?<partition>\\d{5})-"
			+ "(?<offset>\\d{12})\\.gz$"
	);

	private final AmazonS3 s3Client;

	private final Supplier<S3RecordsReader> makeReader;

	private final Map<S3Partition, S3Offset> offsets;

	private final ObjectReader indexParser = new ObjectMapper().reader(ChunksIndex.class);

	private final S3SourceConfig config;

	public S3FilesReader(S3SourceConfig config, AmazonS3 s3Client, Map<S3Partition, S3Offset> offsets, Supplier<S3RecordsReader> recordReader) {
		this.config = config;
		this.offsets = Optional.ofNullable(offsets).orElseGet(HashMap::new);
		this.s3Client = s3Client;
		this.makeReader = recordReader;
	}

	public Iterator<S3SourceRecord> iterator() {
		return readAll();
	}

	public interface PartitionFilter {
		// convenience for simple filters. Only the 2 argument version will ever be called.
		boolean matches(int partition);

		default boolean matches(String topic, int partition) {
			return matches(partition);
		}

		static PartitionFilter from(BiPredicate<String, Integer> filter) {
			return new PartitionFilter() {
				@Override
				public boolean matches(int partition) {
					throw new UnsupportedOperationException();
				}

				@Override
				public boolean matches(String topic, int partition) {
					return filter.test(topic, partition);
				}
			};
		}

		PartitionFilter MATCH_ALL = p -> true;
	}

	private static final Pattern DATA_SUFFIX = Pattern.compile("\\.gz$");

	private int partition(String key) {
		final Matcher matcher = config.keyPattern.matcher(key);
		if (!matcher.find()) {
			throw new IllegalArgumentException("Not a valid chunk filename! " + key);
		}
		return Integer.parseInt(matcher.group("partition"));
	}

	private String topic(String key) {
		final Matcher matcher = config.keyPattern.matcher(key);
		if (!matcher.find()) {
			throw new IllegalArgumentException("Not a valid chunk filename! " + key);
		}
		return matcher.group("topic");
	}

	public Iterator<S3SourceRecord> readAll() {
		return new Iterator<S3SourceRecord>() {
			String currentKey;

			ObjectListing objectListing;
			Iterator<S3ObjectSummary> nextFile = Collections.emptyIterator();
			Iterator<ConsumerRecord<byte[], byte[]>> iterator = Collections.emptyIterator();

			private void nextObject() {
				while (!nextFile.hasNext() && hasMoreObjects()) {

					// partitions will be read completely for each prefix (e.g., a day) in order.
					// i.e., all of partition 0 will be read before partition 1. Seems like that will make perf wonky if
					// there is an active, multi-partition consumer on the other end.
					// to mitigate that, have as many tasks as partitions.
					if (objectListing == null) {
						objectListing = s3Client.listObjects(new ListObjectsRequest(
							config.bucket,
							config.keyPrefix,
							config.startMarker,
							null,
							// we have to filter out chunk indexes on this end, so
							// whatever the requested page size is, we'll need twice that
							config.pageSize * 2
						));
						log.debug("aws ls {}/{} after:{} = {}", config.bucket, config.keyPrefix, config.startMarker,
							LazyString.of(() -> objectListing.getObjectSummaries().stream().map(S3ObjectSummary::getKey).collect(toList())));
					} else {
						String marker = objectListing.getNextMarker();
						objectListing = s3Client.listNextBatchOfObjects(objectListing);
						log.debug("aws ls {}/{} after:{} = {}", config.bucket, config.keyPrefix, marker,
							LazyString.of(() -> objectListing.getObjectSummaries().stream().map(S3ObjectSummary::getKey).collect(toList())));
					}

					List<S3ObjectSummary> chunks = new ArrayList<>(objectListing.getObjectSummaries().size() / 2);
					for (S3ObjectSummary chunk : objectListing.getObjectSummaries()) {
						if (DATA_SUFFIX.matcher(chunk.getKey()).find() && parseKeyUnchecked(chunk.getKey(),
								(t, p, o) -> config.partitionFilter.matches(t, p))) {
							S3Offset offset = offset(chunk);
							if (offset != null) {
								// if our offset for this partition is beyond this chunk, ignore it
								// this relies on filename lexicographic order being correct
								if (offset.getS3key().compareTo(chunk.getKey()) > 0) {
									log.debug("Skipping {} because < current offset of {}", chunk.getKey(), offset);
									continue;
								}
							}
							chunks.add(chunk);
						}
					}
					log.debug("Next Chunks: {}", LazyString.of(() -> chunks.stream().map(S3ObjectSummary::getKey).collect(toList())));
					nextFile = chunks.iterator();
				}
				if (!nextFile.hasNext()) {
					iterator = Collections.emptyIterator();
					return;
				}
				try {
					S3ObjectSummary file = nextFile.next();

					currentKey = file.getKey();
					S3Offset offset = offset(file);
					if (offset != null && offset.getS3key().equals(currentKey)) {
						resumeFromOffset(offset);
					} else {
						log.debug("Now reading from {}", currentKey);
						S3RecordsReader reader = makeReader.get();
						InputStream content = getContent(s3Client.getObject(config.bucket, currentKey));
						iterator = parseKey(currentKey, (topic, partition, startOffset) -> {
							reader.init(topic,partition, content, startOffset);
							return reader.readAll(topic, partition, content, startOffset);
						});
					}
				} catch (IOException e) {
					throw new AmazonClientException(e);
				}
			}

			private InputStream getContent(S3Object object) throws IOException {
				return config.inputFilter.filter(object.getObjectContent());
			}

			private S3Offset offset(S3ObjectSummary chunk) {
				return offsets.get(S3Partition.from(config.bucket, config.keyPrefix, topic(chunk.getKey()), partition(chunk.getKey())));
			}

			/**
			 * If we have a non-null offset to resume from, then our marker is the current file, not the next file,
			 * so we need to load the marker and find the offset to start from.
			 */
			private void resumeFromOffset(S3Offset offset) throws IOException {
				log.debug("resumeFromOffset {}", offset);
				S3RecordsReader reader = makeReader.get();

				ChunksIndex index = getChunksIndex(offset.getS3key());
				ChunkDescriptor chunkDescriptor = index.chunkContaining(offset.getOffset() + 1)
					.orElse(null);

				if (chunkDescriptor == null) {
					log.warn("Missing chunk descriptor for requested offset {} (max:{}). Moving on to next file.",
						offset, index.lastOffset());
					// it's possible we were at the end of this file,
					// so move on to the next one
					nextObject();
					return;
				}

				// if we got here, it is a real object and contains
				// the offset we want to start at

				// if need the start of the file for the read, let it read it
				if (reader.isInitRequired() && chunkDescriptor.byte_offset > 0) {
					try (S3Object object = s3Client.getObject(new GetObjectRequest(config.bucket, offset.getS3key()))) {
						parseKey(object.getKey(), (topic, partition, startOffset) -> {
							reader.init(topic, partition, getContent(object), startOffset);
							return null;
						});
					}
				}

				GetObjectRequest request = new GetObjectRequest(config.bucket, offset.getS3key());
				request.setRange(chunkDescriptor.byte_offset, index.totalSize());

				S3Object object = s3Client.getObject(request);

				currentKey = object.getKey();
				log.debug("Resume {}: Now reading from {}, reading {}-{}", offset, currentKey, chunkDescriptor.byte_offset, index.totalSize());

				iterator = parseKey(object.getKey(), (topic, partition, startOffset) ->
					reader.readAll(topic, partition, getContent(object), chunkDescriptor.first_record_offset));

				// skip records before the given offset
				long recordSkipCount = offset.getOffset() - chunkDescriptor.first_record_offset + 1;
				for (int i = 0; i < recordSkipCount; i++) {
					iterator.next();
				}
			}

			@Override
			public boolean hasNext() {
				while (!iterator.hasNext() && hasMoreObjects()) {
					nextObject();
				}
				return iterator.hasNext();
			}

			boolean hasMoreObjects() {
				return objectListing == null || objectListing.isTruncated() || nextFile.hasNext();
			}

			@Override
			public S3SourceRecord next() {
				ConsumerRecord<byte[], byte[]> record = iterator.next();
				return new S3SourceRecord(
					S3Partition.from(config.bucket, config.keyPrefix, record.topic(), record.partition()),
					S3Offset.from(currentKey, record.offset()),
					record.topic(),
					record.partition(),
					record.key(),
					record.value()
				);
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	private <T> T parseKeyUnchecked(String key, QuietKeyConsumer<T> consumer) {
		try {
			return parseKey(key, consumer::consume);
		} catch (IOException never) {
			throw new RuntimeException(never);
		}
	}

	private <T> T parseKey(String key, KeyConsumer<T> consumer) throws IOException {
		final Matcher matcher = config.keyPattern.matcher(key);
		if (!matcher.find()) {
			throw new IllegalArgumentException("Not a valid chunk filename! " + key);
		}
		final String topic = matcher.group("topic");
		final int partition = Integer.parseInt(matcher.group("partition"));
		final long startOffset = Long.parseLong(matcher.group("offset"));

		return consumer.consume(topic, partition, startOffset);
	}


	private interface QuietKeyConsumer<T> {
		T consume(String topic, int partition, long startOffset);
	}

	private interface KeyConsumer<T> {
		T consume(String topic, int partition, long startOffset) throws IOException;
	}

	private ChunksIndex getChunksIndex(String key) throws IOException {
		return indexParser.readValue(new InputStreamReader(s3Client.getObject(config.bucket, DATA_SUFFIX.matcher(key)
			.replaceAll(".index.json")).getObjectContent()));
	}

	/**
	 * Filtering applied to the S3InputStream. Will almost always start
	 * with GUNZIP, but could also include things like decryption.
	 */
	public interface InputFilter {
		InputStream filter(InputStream inputStream) throws IOException;

		InputFilter GUNZIP = GZIPInputStream::new;
	}

}
