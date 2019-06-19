package com.joey.sa.mdfs.namenode.NameNode;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;


@SpringBootApplication
@EnableEurekaServer
@EnableDiscoveryClient
public class NameNodeApplication {

	public static void main(String[] args) {
		SpringApplication.run(NameNodeApplication.class, args);
	}

}
