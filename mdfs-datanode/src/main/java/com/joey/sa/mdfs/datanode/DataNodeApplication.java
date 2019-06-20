package com.joey.sa.mdfs.datanode;

import com.joey.sa.mdfs.datanode.Database.DatabaseProperty;
import com.joey.sa.mdfs.datanode.Database.DatabaseService;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableConfigurationProperties(DatabaseProperty.class)
public class DataNodeApplication {

	public static void main(String[] args){
		SpringApplication.run(DataNodeApplication.class, args);
	}

	@Bean
	CommandLineRunner init(DatabaseService databaseService) {
		return (args) -> {
			databaseService.deleteAll();
			databaseService.init();
		};
	}

}
