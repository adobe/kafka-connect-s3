package com.spredfast.kafka.connect.s3;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

/**
 * Encodes raw bytes, prefixed by a 4 byte, big-endian integer
 * indicating the length of the byte sequence.
 */
public class ByteLengthFormat implements S3RecordFormat, Configurable {

	private static final int LEN_SIZE = 4;
	private static final byte[] NO_BYTES = {};

	private boolean includesKeys;

	public ByteLengthFormat() {
	}

	public ByteLengthFormat(boolean includesKeys) {
		this.includesKeys = includesKeys;
	}

	@Override
	public void configure(Map<String, ?> configs) {
		includesKeys = Optional.ofNullable(configs.get("include.keys")).map(Object::toString)
			.map(Boolean::valueOf).orElse(false);
	}

	@Override
	public S3RecordsWriter newWriter() {
		return records -> records.map(this::encode);
	}

	private byte[] encode(ProducerRecord<byte[], byte[]> r) {
		// write optionally the key, and the value, each preceded by their length
		Optional<byte[]> key = Optional.ofNullable(r.key()).filter(k -> includesKeys);
		byte[] value = Optional.ofNullable(r.value()).orElse(NO_BYTES);
		byte[] result = new byte[LEN_SIZE + value.length + (includesKeys ? key.map(arr -> arr.length).orElse(0) + LEN_SIZE : 0)];
		ByteBuffer wrapped = ByteBuffer.wrap(result);
		key.ifPresent(k -> {
			wrapped.putInt(k.length);
			wrapped.put(k);
		});
		wrapped.putInt(value.length);
		wrapped.put(value);
		return result;
	}

	@Override
	public S3RecordsReader newReader() {
		return new BytesRecordReader(includesKeys);
	}

}
