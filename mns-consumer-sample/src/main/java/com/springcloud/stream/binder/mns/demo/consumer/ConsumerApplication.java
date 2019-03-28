package com.springcloud.stream.binder.mns.demo.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
@EnableBinding(Sink.class)
public class ConsumerApplication {
	public static void main(String[] args) {
		ConfigurableApplicationContext config = SpringApplication.run(ConsumerApplication.class, args);
		System.out.println("--------Started " + config.getEnvironment().getProperty("spring.application.name"));

	}

	@StreamListener(Sink.INPUT)
	public void recieve(Object payload) {
		System.out.println(payload);
	}
}
