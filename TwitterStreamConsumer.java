package com.tushar.kafka.twitter;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class TwitterStreamConsumer {

	String topicName = "my-output-topic";
	String groupName = "SimpleTopicGroup";
	KafkaConsumer<String, String> consumer = null;

	public void initialize() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", groupName);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<String, String>(props);
	}

	public void consume(){
	    
	    consumer.subscribe(Arrays.asList(topicName));
	    System.out.println("Subscribed to topic " + topicName);
	    
	    while (true){
	            ConsumerRecords<String, String> records = consumer.poll(10);
	            for (ConsumerRecord<String, String> record : records){
	            		//Status status=(Status)record.value();
	            		final String location=record.key();
	                    System.out.println("Location = " +location);
	            }
	    }
	}

	/**
	 * @param keys
	 * @return
	 */
	public static Map<String, Integer> countLocationData(Collection<String> keys) {
		Map<String, Integer> locationData = new HashMap<>();
		for (String string : keys) {
			if (locationData.containsKey(string)) {
				locationData.put(string, locationData.get(string) + 1);
			} else {
				locationData.put(string, 1);
			}
		}
		return locationData;
	}

	public static void main(String[] args) {

		TwitterStreamConsumer consumer = new TwitterStreamConsumer();

		consumer.initialize();

		consumer.consume();

	}

}
