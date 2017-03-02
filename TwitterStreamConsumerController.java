package com.tushar.kafka.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;
import javax.xml.bind.ValidationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.tushar.kafka.core.UserLocation;
import com.tushar.kafka.twitter.KafkaTwitterStreamConsumer;

@Controller
public class TwitterStreamConsumerController {

	//private Logger logger = LoggerFactory.getLogger(TwitterStreamConsumerController.class);
	@Autowired
	@Qualifier(value="streamService")
	private KafkaTwitterStreamConsumer streamService;

	@RequestMapping(method = RequestMethod.GET, value = "/getTwitterAnalysis/{topicName}")
	@ResponseBody
	public List<UserLocation> getTwitterData(@PathVariable("topicName") String topicName) //throws ValidationException 
	{
		//if (null == topicName || "".equals(topicName))
			//throw new ValidationException("Topic name should not be empty");
		//logger.info("Getting data for topic {}", topicName);
		List<UserLocation> locations = null;
		try {
			locations = streamService.consumeFromTopic(topicName);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return locations;
	}

	@ExceptionHandler(ValidationException.class)
	@ResponseBody
	public Map<String, String> errorResponse(Exception ex, HttpServletResponse response) {
		Map<String, String> errorMap = new HashMap<>();
		errorMap.put("errorMessage", ex.getMessage());
		response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR.value());
		return errorMap;
	}
	

	@RequestMapping(method = RequestMethod.GET, value = "index")
	public String getFirstPage() {
		return "static/index.html";
	}

}
