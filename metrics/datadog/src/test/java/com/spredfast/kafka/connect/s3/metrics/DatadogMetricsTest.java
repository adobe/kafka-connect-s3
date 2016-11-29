package com.spredfast.kafka.connect.s3.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.util.Optional;

import org.junit.Test;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spredfast.kafka.connect.s3.Configure;
import com.spredfast.kafka.connect.s3.Metrics;

public class DatadogMetricsTest {

	@Test
	public void configureMetricsDedupes() {
		Metrics a = Configure.metrics(ImmutableMap.of(
			"metrics.reporter", "datadog",
			"metrics.reporter.name", "testA",
			"metrics.reporter.test.noreport", "true"
		));

		Metrics a2 = Configure.metrics(ImmutableMap.of(
			"metrics.reporter", "datadog",
			"metrics.reporter.name", "testA",
			"metrics.reporter.test.noreport", "true"
		));

		assertSame(a, a2);


		Metrics b = Configure.metrics(ImmutableMap.of(
			"metrics.reporter", "datadog",
			"metrics.reporter.name", "testB",
			"metrics.reporter.test.noreport", "true"
		));

		assertNotSame(b, a2);
	}

	@Test
	public void metricsAreTagged() {
		DatadogMetrics metrics = new DatadogMetrics(ImmutableMap.of(
			"name", "testTagging",
			"test.noreport", "true"
		));

		// create two histograms with the same name and different tags
		metrics.hist(1, "foo", ImmutableMap.of("foo", "bar"));
		metrics.hist(2, "foo", ImmutableMap.of("foo", "baz"));

		// and a meter with multiple tags
		metrics.meter(2, "fooMeter", ImmutableMap.of("foo", "baz", "bar", "qux"));
		metrics.meter(2, "fooMeter", ImmutableMap.of("bar", "qux", "foo", "baz"));

		// and show that we create distinct metrics
		assertEquals(ImmutableSet.of(
			"foo[foo:bar]",
			"foo[foo:baz]"
		), metrics.getRegistry().getHistograms().keySet());

		// and order our tags correctly
		assertEquals(ImmutableSet.of(
			"fooMeter[bar:qux,foo:baz]"
		), metrics.getRegistry().getMeters().keySet());
	}

	@Test
	public void parseTags() {
		assertEquals(Optional.of(ImmutableList.of(
			"a:b","c:d","longer_name:has-a-value"
		)), DatadogMetrics.parseTags(ImmutableMap.of("tags", "a:b,c:d,longer_name:has-a-value")));

		assertEquals(Optional.empty(),
			DatadogMetrics.parseTags(ImmutableMap.of()));

		assertEquals(Optional.empty(),
			DatadogMetrics.parseTags(ImmutableMap.of("tags", "")));
	}
}
