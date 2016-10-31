package com.spredfast.kafka.connect.s3;

import java.util.function.Supplier;

public class LazyString {

	private final Supplier<?> stringValue;
	private transient String string;

	private LazyString(Supplier<?> stringValue) {
		this.stringValue = stringValue;
	}

	public static Object of(Supplier<?> stringValue) {
		return new LazyString(stringValue);
	}

	@Override
	public String toString() {
		if (string == null) {
			string = stringValue.get().toString();
		}
		return string;
	}
}
