package com.spredfast.kafka.connect.s3.json;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ChunksIndex {

	@JsonProperty
	public List<ChunkDescriptor> chunks;

	public static ChunksIndex of(List<ChunkDescriptor> chunks) {
		ChunksIndex index = new ChunksIndex();
		index.chunks = chunks;
		return index;
	}
}
