package com.spredfast.kafka.connect.s3.source;

import static com.spredfast.kafka.connect.s3.S3.s3client;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
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
public class S3FilesReader implements Iterable<SourceRecord> {

	private final String bucket;
	private final String keyPrefix;
	private final AmazonS3 s3Client;

	private final BytesRecordReader reader;
	private final int pageSize;

	private final String startMarker;

	private final Map<S3Partition, S3Offset> offsets;

	private final PartitionFilter partitionFilter;

	private final ObjectReader indexParser = new ObjectMapper().reader(ChunksIndex.class);

	private final InputFilter inputFilter;

	/**
	 * @param bucket           the s3 bucket name
	 * @param keyPrefix        prefix for keys, not including the date. Must match the prefix you configured the sink with.
	 * @param s3Client         s3 client.
	 * @param pageSize         number of s3 objects to load at a time.
	 * @param fileIncludesKeys do the S3 files contain the record keys as well as the values?
	 * @param partitionFilter  filter the partition
	 */
	public S3FilesReader(String bucket, String keyPrefix, AmazonS3 s3Client, int pageSize, boolean fileIncludesKeys, InputFilter inputFilter, PartitionFilter partitionFilter, String startMarker, Map<S3Partition, S3Offset> offsets) {
		this.bucket = bucket;
		this.keyPrefix = S3Partition.normalizePrefix(keyPrefix);
		this.s3Client = s3Client;
		// we have to filter out chunk indexes on this end, so
		// whatever the requested page size is, we'll need twice that
		this.pageSize = pageSize * 2;
		this.reader = new BytesRecordReader(fileIncludesKeys);
		this.startMarker = startMarker;

		this.offsets = offsets == null ? new HashMap<S3Partition, S3Offset>() : offsets;

		this.partitionFilter = partitionFilter == null ? PartitionFilter.MATCH_ALL
			: partitionFilter;
		this.inputFilter = inputFilter == null ? InputFilter.GUNZIP
			: inputFilter;
	}

	public Iterator<SourceRecord> iterator() {
		return readAll();
	}

	public interface PartitionFilter {
		boolean matches(S3ObjectSummary object);

		PartitionFilter MATCH_ALL = object -> true;
	}

	private static final Pattern DATA_SUFFIX = Pattern.compile("\\.gz$");

	public Iterator<SourceRecord> readAll() {
		return new Iterator<SourceRecord>() {
			String currentKey;

			ObjectListing lastObjectListing;
			ObjectListing objectListing;
			Iterator<S3ObjectSummary> nextFile = Collections.emptyIterator();
			Iterator<ConsumerRecord<byte[], byte[]>> iterator = Collections.emptyIterator();

			private void nextObject() {
				while (!nextFile.hasNext() && hasMoreObjects()) {
					ObjectListing last = this.objectListing;

					if (objectListing == null) {
						objectListing = s3Client.listObjects(new ListObjectsRequest(
							bucket,
							keyPrefix,
							// since the date is a prefix, we start with the first object on that day or later
							startMarker,
							null,
							pageSize
						));
					} else {
						objectListing = s3Client.listNextBatchOfObjects(objectListing);
					}

					lastObjectListing = last;
					List<S3ObjectSummary> chunks = new ArrayList<>(objectListing.getObjectSummaries().size() / 2);
					for (S3ObjectSummary chunk : objectListing.getObjectSummaries()) {
						if (DATA_SUFFIX.matcher(chunk.getKey()).find() && partitionFilter.matches(chunk)) {


							S3Offset offset = offset(chunk);
							if (offset != null) {
								// if our offset for this partition is beyond this chunk, ignore it
								// this relies on filename lexicographic order being correct
								if (offset.getS3key().compareTo(chunk.getKey()) > 0) {
									continue;
								}
							}
							chunks.add(chunk);
						}
					}
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
						iterator = reader.readAll(currentKey,
							inputFilter.filter(s3Client.getObject(bucket, currentKey).getObjectContent()));
					}
				} catch (IOException e) {
					throw new AmazonClientException(e);
				}
			}

			private S3Offset offset(S3ObjectSummary chunk) {
				return offsets.get(S3Partition.from(bucket, keyPrefix, reader.partition(chunk.getKey())));
			}

			/**
			 * If we have a non-null offset to resume from, then our marker is the current file, not the next file,
			 * so we need to load the marker and find the offset to start from.
			 */
			private void resumeFromOffset(S3Offset offset) throws IOException {

				ChunkDescriptor chunkDescriptor = findOffsetChunk(offset.getS3key(), offset.getOffset());

				if (chunkDescriptor == null) {
					// it's possible we were at the end of this file,
					// so move on to the next one
					nextObject();
					return;
				}

				// if we got here, it is a real object and contains
				// the offset we want to start at
				GetObjectRequest request = new GetObjectRequest(bucket, offset.getS3key());
				request.setRange(chunkDescriptor.byte_offset);

				S3Object object = s3Client.getObject(request);

				currentKey = object.getKey();
				iterator = reader.readAll(object.getKey(), inputFilter.filter(object.getObjectContent()));

				// skip records before the requested offset
				long recordSkipCount = offset.getOffset() - chunkDescriptor.first_record_offset;
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
				return objectListing == null || objectListing.isTruncated();
			}

			@Override
			public SourceRecord next() {
				ConsumerRecord<byte[], byte[]> record = iterator.next();
				return new SourceRecord(
					S3Partition.from(bucket, keyPrefix, record.partition()).asMap(),
					S3Offset.from(currentKey, record.offset()).asMap(),
					record.topic(),
					record.partition(),
					Schema.OPTIONAL_BYTES_SCHEMA,
					record.key(),
					Schema.BYTES_SCHEMA,
					record.value()
				);
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	private ChunkDescriptor findOffsetChunk(String key, long offset) throws IOException {
		ChunksIndex index = indexParser.readValue(new InputStreamReader(s3Client.getObject(bucket, DATA_SUFFIX.matcher(key)
			.replaceAll(".index.json")).getObjectContent()));
		int chunk = 0;
		while (chunk < index.chunks.size()
			&& offset > index.chunks.get(chunk).first_record_offset
			+ index.chunks.get(chunk).num_records) {
			chunk++;
		}
		if (chunk >= index.chunks.size()) {
			return null;
		}
		return index.chunks.get(chunk);
	}

	/**
	 * Filtering applied to the S3InputStream. Will almost always start
	 * with GUNZIP, but could also include things like decryption.
	 */
	public interface InputFilter {
		InputStream filter(InputStream inputStream) throws IOException;

		InputFilter GUNZIP = new InputFilter() {
			@Override
			public InputStream filter(InputStream inputStream) throws IOException {
				return new GZIPInputStream(inputStream);
			}
		};
	}

	/**
	 * This is just for testing. It reads in all records
	 * from the s3 bucket configured by the properties file you give it
	 * and it writes out all the raw values. Unless your values all
	 * have a delimiter at the end, this is probably not useful to you.
	 */
	public static void main(String... args) throws IOException {
		if (args.length != 1) {
			System.err.println("Usage: java ...S3FileReader <config.properties>");
			System.exit(255);
		}
		Properties config = new Properties();
		config.load(new FileInputStream(args[0]));

		String bucket = config.getProperty("s3.bucket");
		String prefix = config.getProperty("s3.prefix");

		Map<String, String> props = new HashMap<>();
		for (Map.Entry<Object, Object> entry : config.entrySet()) {
			props.put(entry.getKey().toString(), entry.getValue().toString());
		}
		AmazonS3 client = s3client(props);

		S3FilesReader consumerRecords = new S3FilesReader(bucket, prefix, client, 100, true, null, null, null, null);
		for (SourceRecord record : consumerRecords) {
			System.out.write((byte[]) record.value());
		}
		System.out.close();
	}

}
