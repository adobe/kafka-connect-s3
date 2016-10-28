package com.spredfast.kafka.connect.s3.json;

import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ChunksIndex {

	@JsonProperty
	public List<ChunkDescriptor> chunks;

	/**
	 * @return the size of the file (compressed) in bytes.
	 */
	public long totalSize() {
		return last().map(last -> last.byte_offset + last.byte_length).orElse(0L);
	}

	public long lastOffset() {
		return last().map(last -> last.first_record_offset + last.num_records - 1).orElse(-1L);
	}

	private Optional<ChunkDescriptor> last() {
		return chunks.isEmpty() ? Optional.empty() : Optional.of(chunks.get(chunks.size() - 1));
	}

	public static ChunksIndex of(List<ChunkDescriptor> chunks) {
		ChunksIndex index = new ChunksIndex();
		index.chunks = chunks;
		return index;
	}

	public Optional<ChunkDescriptor> chunkContaining(long offset) {
		return chunks.stream().filter(chunk -> chunk.first_record_offset + chunk.num_records > offset)
			.findFirst();
	}
}
