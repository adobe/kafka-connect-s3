package com.spredfast.kafka.connect.s3.metrics;

import static java.util.Optional.ofNullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.coursera.metrics.datadog.DatadogReporter;
import org.coursera.metrics.datadog.transport.HttpTransport;
import org.coursera.metrics.datadog.transport.Transport;
import org.coursera.metrics.datadog.transport.UdpTransport;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.spredfast.kafka.connect.s3.Configure;
import com.spredfast.kafka.connect.s3.Metrics;

/**
 * Reports metrics via https://github.com/coursera/metrics-datadog
 *
 * Config: https://github.com/spredfast/kafka-connect-s3/wiki/Metrics
 *
 */
public class DatadogMetrics implements Metrics {

	private final MetricRegistry registry;

	private final Map<String, Map<Map<String, String>, String>> nameCache = new HashMap<>();

	public DatadogMetrics(Map<String, String> config) {
		registry = new MetricRegistry();

		// don't try to report to DD in tests
		if ("true".equals(config.get("test.noreport"))) {
			return;
		}

		DatadogReporter.Builder builder = DatadogReporter.forRegistry(registry);

		parseTags(config).ifPresent(builder::withTags);

		if (Boolean.valueOf(config.getOrDefault("ec2", "true"))) {
			try {
				builder.withEC2Host();
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}

		ofNullable(config.get("prefix"))
			.ifPresent(builder::withPrefix);

		if ("udp".equalsIgnoreCase(config.getOrDefault("transport", "udp"))) {
			builder.withTransport(buildUdp(Configure.subKeys("transport", config)));
		} else {
			builder.withTransport(buildHttp(Configure.subKeys("transport", config)));
		}

		// don't worry about closing as it only creates a daemon thread
		builder.build().start(Integer.parseInt(config.getOrDefault("frequency", "10")), TimeUnit.SECONDS);
	}

	@VisibleForTesting
	static Optional<List<String>> parseTags(Map<String, String> config) {
		return ofNullable(config.get("tags"))
			.filter(s -> s.length() > 0)
			.map(s -> s.split(","))
			.map(ImmutableList::copyOf);
	}

	private Transport buildUdp(Map<String, Object> transport) {
		UdpTransport.Builder builder = new UdpTransport.Builder();
		ofNullable(transport.get("host"))
			.map(Object::toString)
			.ifPresent(builder::withStatsdHost);
		ofNullable(transport.get("prefix"))
			.map(Object::toString)
			.ifPresent(builder::withPrefix);
		ofNullable(transport.get("port"))
			.map(Object::toString)
			.map(Integer::parseInt)
			.ifPresent(builder::withPort);
		return builder.build();
	}

	private Transport buildHttp(Map<String, Object> transport) {
		HttpTransport.Builder builder = new HttpTransport.Builder();

		builder.withApiKey(ofNullable(transport.get("key"))
			.orElseThrow(() -> new IllegalArgumentException("Missing API Key!")).toString());

		ofNullable(transport.get("connect.timeout.ms"))
			.map(Object::toString)
			.map(Integer::parseInt)
			.ifPresent(builder::withConnectTimeout);

		ofNullable(transport.get("socket.timeout.ms"))
			.map(Object::toString)
			.map(Integer::parseInt)
			.ifPresent(builder::withSocketTimeout);

		// only setup proxy if at least a port is specified
		ofNullable(transport.get("proxy.port"))
			.map(Object::toString)
			.map(Integer::parseInt)
			.ifPresent(port -> builder.withProxy(transport.getOrDefault("proxy.host", "localhost").toString(), port));

		return builder.build();
	}

	@Override
	public void meter(int count, String name, Map<String, String> tags) {
		registry.meter(name(name, tags)).mark(count);
	}

	private String name(String name, Map<String, String> tags) {
		// cache all this string concatenation. hopefully the hashing is a lot cheaper
		return nameCache.computeIfAbsent(name, n -> new HashMap<>())
			.computeIfAbsent(tags, ts -> name + '[' + tags.entrySet().stream()
				.sorted(Map.Entry.comparingByKey())
				.reduce(new StringBuilder(), (sb, tag) -> (sb.length() > 0 ? sb.append(',') : sb)
					.append(tag.getKey()).append(':').append(tag.getValue()), StringBuilder::append)
				.toString() + ']');
	}

	@Override
	public void hist(long value, String name, Map<String, String> tags) {
		registry.histogram(name(name, tags)).update(value);
	}

	@Override
	public void gauge(String name, Map<String, String> tags, Supplier<?> getValue) {
		String key = name(name, tags);
		// only add the gauge if it doesn't already exist
		if (!registry.getGauges().containsKey(key)) {
			synchronized (registry) {
				if (!registry.getGauges().containsKey(key)) {
					registry.register(key, (Gauge<?>) getValue::get);
				}
			}
		}
	}

	public MetricRegistry getRegistry() {
		return registry;
	}

	@Override
	public StopTimer time(String name, Map<String, String> tags) {
		return registry.timer(name(name, tags)).time()::stop;
	}
}
