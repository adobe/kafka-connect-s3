package com.spredfast.kafka.connect.s3;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.netflix.curator.test.InstanceSpec;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.PortBinding;

/**
 * Created by noah on 10/20/16.
 */
public class FakeS3 {
	private static final String IMAGE = "lphoward/fake-s3";
	private static final String CONTAINER_PORT = "4569";
	private final int hostPort;
	private final ContainerCreation container;


	private FakeS3(int hostPort, ContainerCreation container) {
		this.hostPort = hostPort;
		this.container = container;
	}

	public static FakeS3 create(DockerClient dockerClient) throws DockerException, InterruptedException {
		// make sure we have the image
		dockerClient.pull(IMAGE, System.err::println);

		// bind a fakes3 image to a random host port
		int port = InstanceSpec.getRandomPort();
		return new FakeS3(port, dockerClient.createContainer(ContainerConfig.builder()
			.hostConfig(HostConfig.builder().portBindings(ImmutableMap.of(
				CONTAINER_PORT, ImmutableList.of(PortBinding.of("0.0.0.0", port))
			)).build())
			.image(IMAGE)
			.exposedPorts(ImmutableSet.of(CONTAINER_PORT))
			.build()));
	}

	public void start(DockerClient dockerClient) throws DockerException, InterruptedException {
		dockerClient.startContainer(container.id());
	}

	public void close(DockerClient dockerClient) throws DockerException, InterruptedException {
		dockerClient.stopContainer(container.id(), 10);
		dockerClient.removeContainer(container.id());
	}

	public String getEndpoint() {
		return "http://localhost:" + hostPort;
	}
}
