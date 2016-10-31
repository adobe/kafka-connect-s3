package com.spredfast.kafka.connect.s3;

import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.google.common.collect.ImmutableMap;

public class FormatTests {

	public static void roundTrip_singlePartition_fromZero_withNullKeys(S3RecordFormat format, List<String> values) throws IOException {
		roundTrip_singlePartition_fromZero_withNullKeys(format, values, 0L);
	}

	// don't currently have a format that cares about start offset, so overload isn't used
	public static void roundTrip_singlePartition_fromZero_withNullKeys(S3RecordFormat format, List<String> values, long startOffset) throws IOException {
		Stream<ProducerRecord<byte[], byte[]>> records = values.stream().map(v ->
			new ProducerRecord<>("topic", 0, null, v.getBytes())
		);

		List<String> results = roundTrip(format, startOffset, records).stream().map(r -> new String(r.value())).collect(Collectors.toList());

		assertEquals(values, results);
	}

	public static void roundTrip_singlePartition_fromZero_withKeys(S3RecordFormat format, Map<String, String> values, long startOffset) throws IOException {
		Stream<ProducerRecord<byte[], byte[]>> records = values.entrySet().stream().map(entry ->
			new ProducerRecord<>("topic", 0, entry.getKey().getBytes(), entry.getValue().getBytes())
		);

		Map<String, String> results = roundTrip(format, startOffset, records).stream().collect(toMap(
			r -> new String(r.key()),
			r -> new String(r.value())
		));

		assertEquals(values, results);
	}

	private static List<ConsumerRecord<byte[], byte[]>> roundTrip(S3RecordFormat format, long startOffset, Stream<ProducerRecord<byte[], byte[]>> records) throws IOException {
		S3RecordsWriter writer = format.newWriter();

		ByteArrayOutputStream boas = new ByteArrayOutputStream();
		boas.write(writer.init("topic", 0, startOffset));

		writer.writeBatch(records).forEach(b -> boas.write(b, 0, b.length));
		boas.write(writer.finish("topic", 0));

		ByteArrayInputStream in = new ByteArrayInputStream(boas.toByteArray());
		S3RecordsReader reader = format.newReader();
		if (reader.isInitRequired()) {
			reader.init("topic", 0, in, startOffset);
		}
		List<ConsumerRecord<byte[], byte[]>> results = new ArrayList<>();
		reader.readAll("topic", 0, in, startOffset).forEachRemaining(results::add);
		return results;
	}


}
