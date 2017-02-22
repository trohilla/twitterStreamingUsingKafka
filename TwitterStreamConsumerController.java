package com.tushar.kafka.controller;

import java.util.Collections;
import java.util.List;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.tushar.kafka.core.UserLocation;
import com.tushar.kafka.twitter.KafkaTwitterStreamConsumer;
import com.tushar.kafka.twitter.TwitterStream;

@Controller
public class TwitterStreamConsumerController {

	private KafkaTwitterStreamConsumer streamService=new KafkaTwitterStreamConsumer();

	@RequestMapping(method=RequestMethod.GET,value="/getTwitterAnalysis/{topicName}")
	@ResponseBody
	public List<UserLocation> getLocationsList(@PathVariable("topicName")String topicName) {
		try {
			return streamService.consumeFromTopic(topicName);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return Collections.emptyList();
	}
	
	
}
