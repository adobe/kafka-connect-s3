package com.spredfast.kafka.connect.s3;

import java.io.InputStream;
import java.util.Iterator;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Thing that can read out records from a byte stream. A new instance will be created for each
 * partition file and will be used by a single thread. init will be called before readAll if requested.
 */
public interface S3RecordsReader {

	/**
	 * Must return true if you implement init().
	 */
	default boolean isInitRequired() { return false; }

	/**
	 * Allows the reader to initialize itself while positioned at the start of a partition file.
	 * If any non-record bytes, etc. are written to the start of a file, they should be consumed
	 * from the given input stream before init() returns.
	 *
	 * @param topic the topic archive being read.
	 * @param partition partition of the file to be read.
	 * @param inputStream the input stream at the start of the file.
	 * @param startOffset the offset at the start of the file (may be different from the offset passed to readAll!)
	 */
	default void init(String topic, int partition, InputStream inputStream, long startOffset) {}

	Iterator<ConsumerRecord<byte[], byte[]>> readAll(final String topic, final int partition, final InputStream inputStream, final long startOffset);
}
