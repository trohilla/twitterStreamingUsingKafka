package com.tushar.kafka.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.tushar.kafka.twitter.TwitterStream;
import com.tushar.kafka.twitter.TwitterStreamProducer;

@Controller
public class TwitterStreamProducerController {

	private TwitterStreamProducer producerService=new TwitterStreamProducer();

	private TwitterStream stream=new TwitterStream();

	@RequestMapping(method=RequestMethod.POST,value="/generateTwitterAnalysis/{topicName}/{filter}")
	@ResponseBody
	public String generateTwitterData(@PathVariable("topicName")String topicName,
										@PathVariable("filter")String filter){
		try {
			producerService.run(topicName, filter);
			stream.start(topicName);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "created";
	}
}
