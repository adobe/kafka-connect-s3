package com.spredfast.kafka.test;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;

/**
 * JSON converter that doesn't shit itself when given null data to convert.
 * Intended specifically to reduce noise in integration tests when used for offset storage.
 */
public class QuietJsonConverter implements Converter {

	private final JsonConverter delegate = new JsonConverter();

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		delegate.configure(configs, isKey);
	}

	@Override
	public byte[] fromConnectData(String topic, Schema schema, Object value) {
		return delegate.fromConnectData(topic, schema, value);
	}

	@Override
	public SchemaAndValue toConnectData(String topic, byte[] value) {
		if (value == null) {
			return SchemaAndValue.NULL;
		}
		return delegate.toConnectData(topic, value);
	}
}
