package com.spredfast.kafka.connect.s3;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Really simple interface for reporting some performance metrics.
 */
public interface Metrics {

	void meter(int count, String name, Map<String, String> tags);

	void hist(long value, String name, Map<String, String> tags);

	void gauge(String name, Map<String, String> tags, Supplier<?> getValue);

	default StopTimer time(String name, Map<String, String> tags) {
		long start = System.nanoTime();
		return () -> {
			hist(System.nanoTime() - start, name + ".time", tags);
			meter(1, name + ".rate", tags);
		};
	}

	interface StopTimer extends AutoCloseable {
		void stop();

		default void close() {
			stop();
		}
	}

	Metrics NOOP = new NoOp();

	Reporters REGISTRY = new Reporters();

	/**
	 * Get a Metrics instance from the global registry by name, constructing it if it doesn't exist.
	 */
	static Metrics getByName(String name, Class<? extends Metrics> clazz, Map<String, String> config) {
		return REGISTRY.getByName(name, clazz, config);
	}

	class Reporters {
		private Reporters() {}

		private final Map<String, Metrics> registry = new ConcurrentHashMap<>();

		public Metrics getByName(String name, Class<? extends Metrics> clazz, Map<String, String> config) {
			return registry.computeIfAbsent(name, n -> {
				try {
					return clazz.getDeclaredConstructor(Map.class).newInstance(config);
				} catch (Exception e) {
					throw new IllegalArgumentException(name, e);
				}
			});
		}
	}

	class NoOp implements Metrics {

		@Override
		public void meter(int count, String name, Map<String, String> tags) {
		}

		@Override
		public void hist(long value, String name, Map<String, String> tags) {
		}

		@Override
		public void gauge(String name, Map<String, String> tags, Supplier<?> getValue) {
		}

		@Override
		public StopTimer time(String name, Map<String, String> tags) {
			return () -> {};
		}
	}
}
