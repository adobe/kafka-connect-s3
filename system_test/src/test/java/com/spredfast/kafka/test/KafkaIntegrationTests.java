/*
  Copyright 2020 Adobe. All rights reserved.
  This file is licensed to you under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License. You may obtain a copy
  of the License at http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software distributed under
  the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
  OF ANY KIND, either express or implied. See the License for the specific language
  governing permissions and limitations under the License.
*/
package com.spredfast.kafka.test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import com.google.common.base.Functions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.netflix.curator.test.InstanceSpec;
import com.netflix.curator.test.TestingServer;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import scala.Option;
import scala.collection.JavaConversions;

public class KafkaIntegrationTests {

	private static final int SLEEP_INTERVAL = 300;

	public static Kafka givenLocalKafka() throws Exception {
		return new Kafka();
	}

	public static void givenLocalKafka(int kafkaPort, IntConsumer localPort) throws Exception {
		try (Kafka kafka = givenLocalKafka()) {
			localPort.accept(kafka.localPort());
		}
	}

	public static void givenKafkaConnect(int kafkaPort, Consumer<Herder> consumer) throws Exception {
		try (KafkaConnect connect = givenKafkaConnect(kafkaPort)) {
			consumer.accept(connect.herder());
		}
	}

	public static void waitForPassing(Duration timeout, Runnable test) {
		waitForPassing(timeout, () -> {
			test.run();
			return null;
		});
	}

	public static <T> T waitForPassing(Duration timeout, Callable<T> test) {
		AssertionError last = null;
		for (int i = 0; i < timeout.toMillis() / SLEEP_INTERVAL; i++) {
			try {
				return test.call();
			} catch (AssertionError e) {
				last = e;
				try {
					Thread.sleep(SLEEP_INTERVAL);
				} catch (InterruptedException e1) {
					Throwables.propagate(e1);
				}
			} catch (Exception e) {
				Throwables.propagate(e);
			}
		}
		if (last != null) {
			throw last;
		}
		return null;
	}

	public static KafkaConnect givenKafkaConnect(int kafkaPort) throws IOException {
		return givenKafkaConnect(kafkaPort, ImmutableMap.of());
	}

	public static KafkaConnect givenKafkaConnect(int kafkaPort, Map<? extends String, ? extends String> overrides) throws IOException {
		File tempFile = File.createTempFile("connect", "offsets");
		System.err.println("Storing offsets at " + tempFile);
		HashMap<String, String> props = new HashMap<>(ImmutableMap.<String, String>builder()
			.put("bootstrap.servers", "localhost:" + kafkaPort)
			// perform no conversion
			.put("key.converter", "com.spredfast.kafka.connect.s3.AlreadyBytesConverter")
			.put("value.converter", "com.spredfast.kafka.connect.s3.AlreadyBytesConverter")
			.put("internal.key.converter", QuietJsonConverter.class.getName())
			.put("internal.value.converter", QuietJsonConverter.class.getName())
			.put("internal.key.converter.schemas.enable", "true")
			.put("internal.value.converter.schemas.enable", "true")
			.put("offset.storage.file.filename", tempFile.getCanonicalPath())
			.put("offset.flush.interval.ms", "1000")
			.put("consumer.metadata.max.age.ms", "1000")
			.put("rest.port", "" + InstanceSpec.getRandomPort())
			.build()
		);
		props.putAll(overrides);

		return givenKafkaConnect(props);
	}

	private static KafkaConnect givenKafkaConnect(Map<String, String> props) {
		WorkerConfig config = new StandaloneConfig(props);
		Plugins plugins = new Plugins(props);
		ConnectorClientConfigOverridePolicy clientConfigOverridePolicy = new AllConnectorClientConfigOverridePolicy();
		Worker worker = new Worker("1", new SystemTime(), plugins, config, new FileOffsetBackingStore(), clientConfigOverridePolicy);
		Herder herder = new StandaloneHerder(worker, "test-cluster", clientConfigOverridePolicy);
		RestServer restServer = new RestServer(config);
		Connect connect = new Connect(herder, restServer);
		connect.start();
		return new KafkaConnect(connect, herder, () -> givenKafkaConnect(props));
	}

	public static class KafkaConnect implements AutoCloseable {

		private final Connect connect;
		private final Herder herder;
		private final Supplier<KafkaConnect> restart;

		public KafkaConnect(Connect connect, Herder herder, Supplier<KafkaConnect> restart) {
			this.connect = connect;
			this.herder = herder;
			this.restart = restart;
		}

		@Override
		public void close() throws Exception {
			connect.stop();
			connect.awaitStop();
		}

		public KafkaConnect restart() {
			return restart.get();
		}

		public Herder herder() {
			return herder;
		}
	}

	public static class Kafka implements AutoCloseable {
		private final TestingServer zk;
		private final KafkaServer kafkaServer;
		private final AdminClient adminClient;

		public Kafka() throws Exception {
			zk = new TestingServer();
			File tmpDir = Files.createTempDir();
			KafkaConfig config = new KafkaConfig(Maps.transformValues(ImmutableMap.<String, Object>builder()
				.put("port", InstanceSpec.getRandomPort())
				.put("broker.id", "1")
				.put("offsets.topic.replication.factor", 1)
				.put("log.dir", tmpDir.getCanonicalPath())
				.put("zookeeper.connect", zk.getConnectString())
				.build(), Functions.toStringFunction()));
			kafkaServer = new KafkaServer(config, Time.SYSTEM, Option.empty(), JavaConversions.asScalaBuffer(ImmutableList.of()));
			kafkaServer.startup();

			Properties adminClientProperties = new Properties();
			adminClientProperties.put("bootstrap.servers", "localhost:" + config.get("port"));
			adminClient = AdminClient.create(adminClientProperties);
		}

		public int localPort() {
			return kafkaServer.config().advertisedPort();
		}

		@Override
		public void close() throws Exception {
			kafkaServer.shutdown();
			kafkaServer.awaitShutdown();
			zk.close();
		}

		public String createUniqueTopic(String prefix) throws InterruptedException, ExecutionException {
			return createUniqueTopic(prefix, 1);
		}

		public String createUniqueTopic(String prefix, int partitions) throws InterruptedException, ExecutionException {
			return createUniqueTopic(prefix, partitions, new Properties());
		}

		public String createUniqueTopic(String prefix, int partitions, Properties topicConfig) throws InterruptedException, ExecutionException {
			checkReady();
			String topic = (prefix + UUID.randomUUID().toString().substring(0, 5)).replaceAll("[^a-zA-Z0-9._-]", "_");
			NewTopic newTopic = new NewTopic(topic, partitions, (short) 1).configs(new HashMap(topicConfig));
			adminClient.createTopics(Collections.singleton(newTopic), new CreateTopicsOptions().timeoutMs(5000));

			adminClient.describeTopics(Collections.singleton(topic))
				.values()
				.get(topic)
				.get()
				.partitions()
				.forEach(p -> assertNotNull(p.leader()));

			return topic;
		}

		public void updateTopic(String topic, Properties topicConfig) {

			ConfigResource configResource = new ConfigResource(Type.TOPIC, topic);
			Collection<ConfigEntry> entries = topicConfig.entrySet()
				.stream()
				.map(entry -> new ConfigEntry((String) entry.getKey(), (String) entry.getValue()))
				.collect(Collectors.toList());

			Config config = new Config(entries);
			adminClient.alterConfigs(Collections.singletonMap(configResource, config));
		}

		public void checkReady() throws InterruptedException {
			checkReady(Duration.ofSeconds(15));
		}

		public void checkReady(Duration timeout) throws InterruptedException {
			waitForPassing(timeout, () -> {
				try {
					assertNotNull(adminClient.describeCluster().clusterId().get());
				} catch (InterruptedException | ExecutionException exception) {
					fail(exception.getMessage());
				}
			});
		}
	}

}
