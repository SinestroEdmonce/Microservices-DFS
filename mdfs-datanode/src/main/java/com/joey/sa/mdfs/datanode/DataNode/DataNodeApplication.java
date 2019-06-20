package com.joey.sa.mdfs.datanode.DataNode;

import com.joey.sa.mdfs.datanode.Database.DatabaseProperty;
import com.joey.sa.mdfs.datanode.Database.DatabaseService;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableConfigurationProperties(DatabaseProperty.class)
public class DataNodeApplication {

	public static void main(String[] args){
		SpringApplication.run(DataNodeApplication.class, args);
	}

	@Bean
	public CommandLineRunner runner(DatabaseService databaseService){
		return new CommandLineRunner() {
			@Override
			public void run(String... args) throws Exception {
				System.out.println(databaseService.getClass());
				databaseService.deleteAll();
				databaseService.init();
			}
		};
	}

}
