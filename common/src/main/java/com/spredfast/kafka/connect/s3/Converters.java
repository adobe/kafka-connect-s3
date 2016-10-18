package com.spredfast.kafka.connect.s3;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.storage.Converter;

public abstract class Converters {

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
			Map<String, Object> subKeys = new LinkedHashMap<>();
			String prefix = classNameProp + ".";
			for (String p : props.keySet()) {
				if (p.startsWith(prefix)) {
					subKeys.put(p.substring(prefix.length()), props.get(p));
				}
			}

			converter.configure(subKeys, isKey);

			return converter;
		} catch (Exception e) {
			throw new IllegalArgumentException("Could not create S3 converter for props " + classNameProp + " isKey=" + isKey);
		}
	}
}
