package com.victor.sql_api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class SqlApiApplication {

	public static void main(String[] args) {
		SpringApplication.run(SqlApiApplication.class, args);
	}

}
