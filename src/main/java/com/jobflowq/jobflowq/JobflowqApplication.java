package com.jobflowq.jobflowq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class JobflowqApplication {

	public static void main(String[] args) {
		SpringApplication.run(JobflowqApplication.class, args);
	}

}
