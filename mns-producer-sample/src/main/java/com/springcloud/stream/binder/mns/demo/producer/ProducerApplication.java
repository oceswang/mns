package com.springcloud.stream.binder.mns.demo.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@SpringBootApplication
@EnableBinding(Source.class)
@Controller
public class ProducerApplication {
	public static void main(String[] args) {
		SpringApplication.run(ProducerApplication.class, args);
	}
	
	@Autowired
    private Source source;
	
	@ResponseBody
	@RequestMapping("/send/{msg}")
	public boolean send(@PathVariable("msg") String msg) {
		return source.output().send(MessageBuilder.withPayload(msg).build());
	}
}
