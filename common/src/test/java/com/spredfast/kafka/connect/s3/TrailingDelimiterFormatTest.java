package com.spredfast.kafka.connect.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

import javax.xml.bind.DatatypeConverter;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TrailingDelimiterFormatTest {

	@Test
	public void defaults() throws IOException {
		FormatTests.roundTrip_singlePartition_fromZero_withNullKeys(givenFormatWithConfig(ImmutableMap.of()), givenValues());
	}

	@Test
	public void withKeys() throws IOException {
		FormatTests.roundTrip_singlePartition_fromZero_withKeys(givenFormatWithConfig(ImmutableMap.of(
			"key.delimiter", "\t"
		)), ImmutableMap.of(
			"123", "456",
			"abc\ndef", "ghi\tjkl"
		), 0);
	}

	@Test
	public void output() throws IOException {
		// using UTF_16BE because UTF_16 adds a byte order mark to every invocation of getBytes, which is annoying
		TrailingDelimiterFormat format = givenFormatWithConfig(ImmutableMap.of(
			"key.delimiter", "\t",
			"key.encoding", "UTF-16BE",
			"value.encoding", "UTF-16BE"
		));

		assertBytesAreEqual("abc\tdef\n".getBytes(Charsets.UTF_16BE), format.newWriter().writeBatch(Stream.of(
			new ProducerRecord<>("topic", "abc".getBytes(Charsets.UTF_16BE), "def".getBytes(Charsets.UTF_16BE))
		)).findFirst().get());
	}

	private void assertBytesAreEqual(byte[] expected, byte[] actual) {
		if (!Arrays.equals(expected, actual)) {
			assertEquals(DatatypeConverter.printHexBinary(expected), DatatypeConverter.printHexBinary(actual));
		}
	}

	private ImmutableList<String> givenValues() {
		return ImmutableList.of("abcd", "567\tav", "2384732109847123098471092384723109847239084732409854329865293847549837");
	}

	private TrailingDelimiterFormat givenFormatWithConfig(ImmutableMap<String, Object> configs) {
		TrailingDelimiterFormat format = new TrailingDelimiterFormat();
		format.configure(configs);
		return format;
	}

}
