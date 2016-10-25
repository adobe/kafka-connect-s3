package com.spredfast.kafka.connect.s3;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.errors.DataException;

/**
 * Record oriented version of RecordsReader. Can be used to simplify your implementation.
 */
public interface RecordReader extends S3RecordsReader {

	ConsumerRecord<byte[], byte[]> read(String topic, int partition, long offset, BufferedInputStream data) throws IOException;

	static RecordReader of(RecordReader r) { return r; }

	@Override
	default Iterator<ConsumerRecord<byte[], byte[]>> readAll(final String topic, final int partition, final InputStream inputStream, final long startOffset) {
		return new Iterator<ConsumerRecord<byte[], byte[]>>() {
			ConsumerRecord<byte[], byte[]> next;

			final BufferedInputStream buffered = new BufferedInputStream(inputStream);

			long offset = startOffset;

			@Override
			public boolean hasNext() {
				try {
					if (next == null) {
						next = read(topic, partition, offset++, buffered);
					}
				} catch (IOException e) {
					throw new DataException(e);
				}
				return next != null;
			}

			@Override
			public ConsumerRecord<byte[], byte[]> next() {
				if (!hasNext()) {
					throw new NoSuchElementException();
				}
				final ConsumerRecord<byte[], byte[]> record = this.next;
				next = null;
				return record;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

}
