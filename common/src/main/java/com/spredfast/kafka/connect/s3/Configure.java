package com.spredfast.kafka.connect.s3;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.storage.Converter;

public abstract class Configure {

	private static final Class<? extends S3RecordFormat> DEFAULT_FORMAT = TrailingDelimiterFormat.class;

	private static final Map<?, String> FORMAT_ALIAS = Collections.unmodifiableMap(new HashMap<String, String>() {{
		put("binary", ByteLengthFormat.class.getName());
		put("text", TrailingDelimiterFormat.class.getName());
	}});


	/**
	 * Create and configure a new Converter instance.
	 *
	 * @param props                 the raw config values.
	 * @param classNameProp         the name of the property that specifies the converter class.
	 *                              Any sub properties will be passed to the converter as config.
	 * @param isKey                 if this converter is for a key or not.
	 * @param defaultConverterClass the default converter class to create and configure if the prop is missing.
	 *                              If null, the result may be null.
	 * @return a new converter, already configured.
	 */
	public static Converter buildConverter(Map<String, ?> props, String classNameProp, boolean isKey, Class<? extends Converter> defaultConverterClass) {
		String className = (String) props.get(classNameProp);

		try {
			Converter converter;

			if (className == null) {
				if (defaultConverterClass == null) {
					return null;
				} else {
					converter = defaultConverterClass.newInstance();
				}
			} else {
				converter = (Converter) Class.forName(className).newInstance();
			}

			if (converter instanceof Configurable) {
				((Configurable) converter).configure(props);
			}

			// grab any properties intended for the converter
			Map<String, Object> subKeys = subKeys(classNameProp, props);

			converter.configure(subKeys, isKey);

			return converter;
		} catch (Exception e) {
			throw new IllegalArgumentException("Could not create S3 converter for props " + classNameProp + " isKey=" + isKey, e);
		}
	}

	public static Map<String, Object> subKeys(String classNamePro, Map<String, ?> props) {
		Map<String, Object> subKeys = new LinkedHashMap<>();
		String prefix = classNamePro + ".";
		for (String p : props.keySet()) {
			if (p.startsWith(prefix)) {
				subKeys.put(p.substring(prefix.length()), props.get(p));
			}
		}
		return subKeys;
	}

	public static S3RecordFormat createFormat(Map<String, String> props) {
		try {
			S3RecordFormat recordFormat = (S3RecordFormat) Optional.ofNullable(props.get("format")).map(Object::toString)
				.map(name -> FORMAT_ALIAS.getOrDefault(name, name))
				.map(className -> {
					try {
						return Class.forName(className);
					} catch (ClassNotFoundException e) {
						throw new IllegalArgumentException(e);
					}
				})
				.orElseGet(() -> (Class) DEFAULT_FORMAT)
				.newInstance();
			if (recordFormat instanceof Configurable) {
				((Configurable) recordFormat).configure(subKeys("format", props));
			}
			return recordFormat;
		} catch (Exception e) {
			throw new ConnectException("Failed to create format: " + props.get("format"), e);
		}
	}

}
