package com.spredfast.kafka.connect.s3.json;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ChunkDescriptor {

	@JsonProperty
	public long byte_length_uncompressed;

	@JsonProperty
	public long num_records;

	@JsonProperty
	public long byte_length;

	@JsonProperty
	public long byte_offset;

	@JsonProperty
	public long first_record_offset;

}
