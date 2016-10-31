package com.spredfast.kafka.connect.s3;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.errors.DataException;
import com.spredfast.kafka.connect.s3.RecordReader;

/**
 * Helper for reading raw length encoded records from a chunk file. Not thread safe.
 */
public class BytesRecordReader implements RecordReader {

	private final ByteBuffer lenBuffer = ByteBuffer.allocate(4);

	private final boolean includesKeys;

	/**
	 * @param includesKeys do the serialized records include keys? Or just values?
	 */
	public BytesRecordReader(boolean includesKeys) {
		this.includesKeys = includesKeys;
	}

	/**
	 * Reads a record from the given uncompressed data stream.
	 *
	 * @return a raw ConsumerRecord or null if at the end of the data stream.
	 */
	@Override
	public ConsumerRecord<byte[], byte[]> read(String topic, int partition, long offset, BufferedInputStream data) throws IOException {
		final byte[] key;
		final int valSize;
		if (includesKeys) {
			// if at the end of the stream, return null
			final Integer keySize = readLen(topic, partition, offset, data);
			if (keySize == null) {
				return null;
			}
			key = readBytes(keySize, data, topic, partition, offset);
			valSize = readValueLen(topic, partition, offset, data);
		} else {
			key = null;
			Integer vSize = readLen(topic, partition, offset, data);
			if (vSize == null) {
				return null;
			}
			valSize = vSize;
		}

		final byte[] value = readBytes(valSize, data, topic, partition, offset);

		return new ConsumerRecord<>(topic, partition, offset, key, value);
	}

	private int readValueLen(String topic, int partition, long offset, InputStream data) throws IOException {
		final Integer len = readLen(topic, partition, offset, data);
		if (len == null) {
			die(topic, partition, offset);
		}
		return len;
	}

	private byte[] readBytes(int keySize, InputStream data, String topic, int partition, long offset) throws IOException {
		final byte[] bytes = new byte[keySize];
		int read = 0;
		while (read < keySize) {
			final int readNow = data.read(bytes, read, keySize - read);
			if (readNow == -1) {
				die(topic, partition, offset);
			}
			read += readNow;
		}
		return bytes;
	}

	private Integer readLen(String topic, int partition, long offset, InputStream data) throws IOException {
		lenBuffer.rewind();
		int read = data.read(lenBuffer.array(), 0, 4);
		if (read == -1) {
			return null;
		} else if (read != 4) {
			die(topic, partition, offset);
		}
		return lenBuffer.getInt();
	}


	protected ConsumerRecord<byte[], byte[]> die(String topic, int partition, long offset) {
		throw new DataException(String.format("Corrupt record at %s-%d:%d", topic, partition, offset));
	}

}
