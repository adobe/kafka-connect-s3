package com.spredfast.kafka.connect.s3.source;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.spredfast.kafka.connect.s3.Configure;
import com.spredfast.kafka.connect.s3.Constants;

public class S3SourceConnector extends SourceConnector {

	private static final int DEFAULT_PARTITION_COUNT = 200;
	private static final String MAX_PARTITION_COUNT = "max.partition.count";
	private Map<String, String> config;

	@Override
	public String version() {
		return Constants.VERSION;
	}

	@Override
	public void start(Map<String, String> config) {
		this.config = config;
	}

	@Override
	public Class<? extends Task> taskClass() {
		return S3SourceTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int taskCount) {
		int partitions = Optional.ofNullable(config.get(MAX_PARTITION_COUNT))
			.map(Integer::parseInt).orElse(DEFAULT_PARTITION_COUNT);

		return IntStream.range(0, taskCount).mapToObj(taskNum ->
			// each task gets every nth partition
			IntStream.iterate(taskNum, i -> i + taskCount)
			.mapToObj(Integer::toString)
			.limit(partitions / taskCount + 1).collect(joining(",")))
			.map(parts -> {
				Map<String, String> task = new HashMap<>(config);
				task.put("partitions", parts);
				return task;
			})
			.collect(toList());
	}

	@Override
	public void stop() {

	}

	@Override
	public ConfigDef config() {
		// TODO
		return new ConfigDef();
	}
}
