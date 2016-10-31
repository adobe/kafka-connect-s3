package com.spredfast.kafka.connect.s3;

import java.io.IOException;

import org.junit.Test;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class ByteLengthFormatTest {

	@Test
	public void defaults() throws IOException {
		FormatTests.roundTrip_singlePartition_fromZero_withNullKeys(givenFormatWithConfig(ImmutableMap.of()), givenValues());
	}

	@Test
	public void withKeys() throws IOException {
		FormatTests.roundTrip_singlePartition_fromZero_withNullKeys(givenFormatWithConfig(ImmutableMap.of("include.keys", "true")), givenValues());
	}


	private ImmutableList<String> givenValues() {
		return ImmutableList.of("abcd", "567\tav", "238473210984712309\n84710923847231098472390847324098543298652938475\n49837");
	}

	private ByteLengthFormat givenFormatWithConfig(ImmutableMap<String, Object> configs) {
		ByteLengthFormat format = new ByteLengthFormat();
		format.configure(configs);
		return format;
	}

}
