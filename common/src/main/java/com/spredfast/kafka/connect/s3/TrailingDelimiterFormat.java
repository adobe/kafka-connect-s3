package com.spredfast.kafka.connect.s3;

import static java.util.stream.Collectors.toList;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Configurable;

/**
 * Reads/writes records to/from a delimited, encoded string.
 * Both delimiter and encoding are configurable.
 */
public class TrailingDelimiterFormat implements S3RecordFormat, Configurable {

	public static final Charset DEFAULT_ENCODING = Charset.forName("UTF-8");
	public static final String DEFAULT_DELIMITER = "\n";
	private static final byte[] NO_BYTES = {};
	private byte[] valueDelimiter;
	private Optional<byte[]> keyDelimiter;

	@Override
	public void configure(Map<String, ?> configs) {
		valueDelimiter = Optional.ofNullable(configs.get("value.delimiter"))
			.map(Object::toString)
			.orElse(DEFAULT_DELIMITER)
			.getBytes(parseEncoding(configs, "value.encoding"));

		keyDelimiter = Optional.ofNullable(configs.get("key.delimiter"))
			.map(Object::toString)
			.map(s -> s.getBytes(parseEncoding(configs, "key.encoding")));

		if (!keyDelimiter.isPresent() && configs.containsKey("key.encoding")) {
			throw new IllegalArgumentException("Key encoding specified without delimiter!");
		}
	}

	private Charset parseEncoding(Map<String, ?> configs, String key) {
		return Optional.ofNullable(configs.get(key))
				.map(Object::toString)
				.map(Charset::forName)
				.orElse(DEFAULT_ENCODING);
	}

	private byte[] encode(ProducerRecord<byte[], byte[]> record) {
		List<byte[]> bytes = Stream.of(
			Optional.ofNullable(record.key()).filter(r -> keyDelimiter.isPresent()),
			keyDelimiter,
			Optional.ofNullable(record.value()),
			Optional.of(valueDelimiter)
		).map(o -> o.orElse(NO_BYTES)).filter(arr -> arr.length > 0).collect(toList());
		int size = bytes.stream().map(arr -> arr.length).reduce(0, (a,b) -> a + b);
		byte[] result = new byte[size];
		for (int i = 0, written = 0; i < bytes.size(); i++) {
			byte[] src = bytes.get(i);
			System.arraycopy(src, 0, result, written, src.length);
			written += src.length;
		}
		return result;
	}

	@Override
	public S3RecordsWriter newWriter() {
		return records -> records.map(this::encode);
	}

	@Override
	public S3RecordsReader newReader() {
		return new DelimitedRecordReader(valueDelimiter, keyDelimiter);
	}

}
