package com.spredfast.kafka.connect.s3;

/**
 * Pairing of reader and writer for records in S3. A new reader/writer will be constructed for each
 * file to be read/written.
 */
public interface S3RecordFormat {

	/**
	 * Returns a function that takes a topic and raw bytes, and returns the bytes to write to S3.
	 * Bytes for each record will be written consecutively without any additional delimiter, so the
	 * reader must be able to read a concatenation of such byte sequences.
	 */
	S3RecordsWriter newWriter();

	/**
	 * @return a reader that can reverse {@link #newWriter()}
	 */
	S3RecordsReader newReader();

	/**
	 * Convenience method if you have your own S3 data you want to read out.
	 */
	static S3RecordFormat readOnly(S3RecordsReader reader) {
		return from(reader, r -> { throw new UnsupportedOperationException("Format is read-only."); });
	}

	static S3RecordFormat from(S3RecordsReader reader, S3RecordsWriter writer) {
		return new S3RecordFormat() {
			@Override
			public S3RecordsWriter newWriter() {
				return writer;
			}

			@Override
			public S3RecordsReader newReader() {
				return reader;
			}
		};
	}
}

