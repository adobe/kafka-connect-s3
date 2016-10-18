package com.spredfast.kafka.connect.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListNextBatchOfObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.spredfast.kafka.connect.s3.sink.BlockGZIPFileWriter;
import com.spredfast.kafka.connect.s3.source.BytesRecordReader;
import com.spredfast.kafka.connect.s3.source.S3FilesReader;
import com.spredfast.kafka.connect.s3.source.S3Offset;
import com.spredfast.kafka.connect.s3.source.S3Partition;

import junit.framework.TestCase;

/**
 * Covers S3 and reading raw byte records. Closer to an integration test.
 */
public class S3FilesReaderTest {

	@Test
	public void testReadingBytesFromS3() throws IOException {
		final Path dir = Files.createTempDirectory("s3FilesReaderTest");
		givenSomeData(dir);

		final AmazonS3 client = givenAMockS3Client(dir);

		List<String> results = whenTheRecordsAreRead(client, true);

		thenTheyAreFilteredAndInOrder(results);
	}

	@Test
	public void testReadingBytesFromS3_withOffsets() throws IOException {
		final Path dir = Files.createTempDirectory("s3FilesReaderTest");
		givenSomeData(dir);

		final AmazonS3 client = givenAMockS3Client(dir);

		List<String> results = whenTheRecordsAreRead(givenAReaderWithOffsets(client,
			"prefix/2015-12-31/topic-00003-000000000001.gz", 5L, "00003"));

		assertEquals(Arrays.asList(
			"willbe=skipped5",
			"willbe=skipped6",
			"willbe=skipped7",
			"willbe=skipped8",
			"willbe=skipped9"
		), results);
	}


	@Test
	public void testReadingBytesFromS3_withOffsetsAtEndOfFile() throws IOException {
		final Path dir = Files.createTempDirectory("s3FilesReaderTest");
		givenSomeData(dir);

		final AmazonS3 client = givenAMockS3Client(dir);

		// this file will be skipped
		List<String> results = whenTheRecordsAreRead(givenAReaderWithOffsets(client,
			"prefix/2015-12-30/topic-00003-000000000000.gz", 1L, "00003"));

		assertEquals(Arrays.asList(
			"willbe=skipped1",
			"willbe=skipped2",
			"willbe=skipped3",
			"willbe=skipped4",
			"willbe=skipped5",
			"willbe=skipped6",
			"willbe=skipped7",
			"willbe=skipped8",
			"willbe=skipped9"
		), results);
	}

	S3FilesReader givenAReaderWithOffsets(AmazonS3 client, String marker, long nextOffset, final String partition) {
		Map<S3Partition, S3Offset> offsets = new HashMap<>();
		offsets.put(S3Partition.from("bucket", "prefix", Integer.valueOf(partition)),
			S3Offset.from(marker, nextOffset));
		return new S3FilesReader(
			"bucket",
			"prefix",
			client,
			1,
			true,
			null,
			// only consume this partition
			new S3FilesReader.PartitionFilter() {
				@Override
				public boolean matches(S3ObjectSummary object) {
					Matcher matcher = BytesRecordReader.DEFAULT_PATTERN.matcher(object.getKey());
					assertTrue(matcher.find());
					return partition.equals(matcher.group("partition"));
				}
			},
			null,
			offsets
		);
	}

	public static class ReversedStringBytesConverter implements Converter {
		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
			// while we're here, verify that we get our subconfig
			assertEquals(configs.get("requiredProp"), "isPresent");
		}

		@Override
		public byte[] fromConnectData(String topic, Schema schema, Object value) {
			byte[] bytes = value.toString().getBytes(Charset.forName("UTF-8"));
			byte[] result = new byte[bytes.length];
			for (int i = 0; i < bytes.length; i++) {
				result[bytes.length - i - 1] = bytes[i];
			}
			return result;
		}

		@Override
		public SchemaAndValue toConnectData(String topic, byte[] value) {
			throw new UnsupportedOperationException();
		}
	}

	@Test
	public void testReadingBytesFromS3_withoutKeysAndACustomConverter() throws IOException {
		final Path dir = Files.createTempDirectory("s3FilesReaderTest");
		givenSomeData(dir, null, givenACustomConverter(), Schema.STRING_SCHEMA);

		final AmazonS3 client = givenAMockS3Client(dir);

		List<String> results = whenTheRecordsAreRead(client, false);

		theTheyAreReversedAndInOrder(results);
	}

	Converter givenACustomConverter() {
		Map<String, Object> config = new HashMap<>();
		config.put("converter", ByteLengthEncodedConverter.class.getName());
		config.put("converter.converter", ReversedStringBytesConverter.class.getName());
		config.put("converter.converter.requiredProp", "isPresent");
		return Converters.buildConverter(config, "converter", false, null);
	}

	void theTheyAreReversedAndInOrder(List<String> results) {
		List<String> expected = Arrays.asList(
			"0-0eulav",
			"0-1eulav",
			"1-1eulav"
		);
		assertEquals(expected, results);
	}

	private void thenTheyAreFilteredAndInOrder(List<String> results) {
		List<String> expected = Arrays.asList(
			"key0-0=value0-0",
			"key1-0=value1-0",
			"key1-1=value1-1"
		);
		assertEquals(expected, results);
	}

	private List<String> whenTheRecordsAreRead(AmazonS3 client, boolean fileIncludesKeys) {
		S3FilesReader reader = new S3FilesReader(
			"bucket",
			"prefix",
			client,
			1, // small to test pagination
			fileIncludesKeys,
			null,
			null,
			"prefix/2016-01-01",
			null
		);
		return whenTheRecordsAreRead(reader);
	}

	private List<String> whenTheRecordsAreRead(S3FilesReader reader) {
		List<String> results = new ArrayList<>();
		for (SourceRecord record : reader) {
			results.add((record.key() == null ? "" : new String((byte[]) record.key()) + "=") + new String((byte[]) record.value()));
		}
		return results;
	}

	private AmazonS3 givenAMockS3Client(final Path dir) {
		final AmazonS3 client = mock(AmazonS3Client.class);
		when(client.listObjects(any(ListObjectsRequest.class))).thenAnswer(new Answer<ObjectListing>() {
			@Override
			public ObjectListing answer(InvocationOnMock invocationOnMock) throws Throwable {
				final ListObjectsRequest req = (ListObjectsRequest) invocationOnMock.getArguments()[0];
				ObjectListing listing = new ObjectListing();

				final List<File> files = new ArrayList<>();
				Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
					@Override
					public FileVisitResult preVisitDirectory(Path toCheck, BasicFileAttributes attrs) throws IOException {
						if (toCheck.startsWith(dir)) {
							return FileVisitResult.CONTINUE;
						}
						return FileVisitResult.SKIP_SUBTREE;
					}

					@Override
					public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
						String key = key(file.toFile());
						if (req.getMarker() == null
							|| key.compareTo(req.getMarker()) > 0) {
							files.add(file.toFile());
						}
						return FileVisitResult.CONTINUE;
					}
				});

				List<S3ObjectSummary> summaries = new ArrayList<>();
				for (int i = 0; i < req.getMaxKeys() && i < files.size(); i++) {
					S3ObjectSummary summary = new S3ObjectSummary();
					String key = key(files.get(i));
					summary.setKey(key);
					listing.setNextMarker(key);
					summaries.add(summary);
				}

				listing.setMaxKeys(req.getMaxKeys());

				listing.getObjectSummaries().addAll(summaries);
				listing.setTruncated(files.size() > req.getMaxKeys());

				return listing;
			}

			private String key(File file) {
				return file.getAbsolutePath().substring(dir.toAbsolutePath().toString().length() + 1);
			}
		});
		when(client.listNextBatchOfObjects(any(ObjectListing.class))).thenCallRealMethod();
		when(client.listNextBatchOfObjects(any(ListNextBatchOfObjectsRequest.class))).thenCallRealMethod();

		when(client.getObject(anyString(), anyString())).thenAnswer(new Answer<S3Object>() {
			@Override
			public S3Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				String key = (String) invocationOnMock.getArguments()[1];
				return getFile(key, dir);
			}
		});
		when(client.getObject(any(GetObjectRequest.class))).thenAnswer(new Answer<S3Object>() {
			@Override
			public S3Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				String key = ((GetObjectRequest) invocationOnMock.getArguments()[0]).getKey();
				return getFile(key, dir);
			}
		});
		when(client.getObjectMetadata(anyString(), anyString())).thenAnswer(new Answer<S3Object>() {
			@Override
			public S3Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				String key = (String) invocationOnMock.getArguments()[1];
				if (!new File(dir.toString(), key).exists()) {
					AmazonServiceException e = new AmazonServiceException("Nope: " + key);
					e.setErrorCode("NoSuchKey");
					throw e;
				}
				return null;
			}
		});
		return client;
	}

	S3Object getFile(String key, Path dir) throws FileNotFoundException {
		S3Object obj = mock(S3Object.class);
		File file = new File(dir.toString(), key);
		when(obj.getKey()).thenReturn(file.getName());
		S3ObjectInputStream stream = new S3ObjectInputStream(new FileInputStream(file), null);
		when(obj.getObjectContent()).thenReturn(stream);
		return obj;
	}

	private void givenSomeData(Path dir) throws IOException {
		Converter valueConverter = Converters.buildConverter(new HashMap<String, Object>(), "missing", false, ByteLengthEncodedConverter.class);
		givenSomeData(dir, valueConverter, valueConverter, Schema.BYTES_SCHEMA);
	}

	private void givenSomeData(Path dir, Converter keyConverter, Converter valueConverter, Schema valueSchema) throws IOException {
		new File(dir.toFile(), "prefix/2015-12-30").mkdirs();
		new File(dir.toFile(), "prefix/2015-12-31").mkdirs();
		new File(dir.toFile(), "prefix/2016-01-01").mkdirs();
		new File(dir.toFile(), "prefix/2016-01-02").mkdirs();
		try (BlockGZIPFileWriter writer0 = new BlockGZIPFileWriter("topic-00003", dir.toString() + "/prefix/2015-12-31", 1, 512, keyConverter, valueConverter);
			 BlockGZIPFileWriter writer1 = new BlockGZIPFileWriter("topic-00000", dir.toString() + "/prefix/2016-01-01", 0, 512, keyConverter, valueConverter);
			 BlockGZIPFileWriter writer2 = new BlockGZIPFileWriter("topic-00001", dir.toString() + "/prefix/2016-01-02", 0, 512, keyConverter, valueConverter);
			 BlockGZIPFileWriter preWriter1 = new BlockGZIPFileWriter("topic-00003", dir.toString() + "/prefix/2015-12-30", 0, 512, keyConverter, valueConverter);
		) {
			preWriter1.write(new SinkRecord(
				"topic",
				0,
				Schema.BYTES_SCHEMA, "willbe".getBytes(),
				valueSchema, ser("skipped0", valueSchema),
				0
			));

			for (int i = 1; i < 10; i++) {
				writer0.write(new SinkRecord(
					"topic",
					0,
					Schema.BYTES_SCHEMA, "willbe".getBytes(),
					valueSchema, ser("skipped" + i, valueSchema),
					i
				));
			}

			writer1.write(new SinkRecord(
				"topic",
				0,
				Schema.BYTES_SCHEMA, "key0-0".getBytes(),
				valueSchema, ser("value0-0", valueSchema),
				10
			));

			writer2.write(new SinkRecord(
				"topic",
				1,
				Schema.BYTES_SCHEMA, "key1-0".getBytes(),
				valueSchema, ser("value1-0", valueSchema),
				0
			));
			writer2.write(new SinkRecord(
				"topic",
				1,
				Schema.BYTES_SCHEMA, "key1-1".getBytes(),
				valueSchema, ser("value1-1", valueSchema),
				1
			));
		}
	}

	private Object ser(String s, Schema valueSchema) {
		return valueSchema == Schema.BYTES_SCHEMA ?
			s.getBytes() : s;
	}

}
