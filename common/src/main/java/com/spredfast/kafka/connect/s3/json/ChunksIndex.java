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
		if (chunks.isEmpty()) {
			return 0;
		}
		ChunkDescriptor last = chunks.get(chunks.size() - 1);
		return last.byte_offset + last.byte_length;
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
