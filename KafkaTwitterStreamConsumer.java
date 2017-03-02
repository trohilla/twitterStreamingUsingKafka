package com.tushar.kafka.twitter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.Lists;
import com.tushar.kafka.core.UserLocation;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 * @author tushar
 *
 */
public class KafkaTwitterStreamConsumer {

	private String groupName = "SimpleTopicGroup";
	private ConsumerConnector consumerConnector = null;

	/**
	 * To initialize the property and consumerConnector object, so as to consume
	 * data from topic.
	 */
	public void initialize() {
		Properties props = new Properties();
		props.put("zookeeper.connect", "localhost:2181");
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", groupName);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		ConsumerConfig conConfig = new ConsumerConfig(props);
		consumerConnector = Consumer.createJavaConsumerConnector(conConfig);
	}

	/**
	 * It will consume data from topic and will save it in a list.
	 */
	public List<UserLocation> consume(String topicName) {

		// Key = topic name, Value = No. of threads for topic
		Map<String, Integer> topicCount = new HashMap<String, Integer>();
		topicCount.put(topicName, new Integer(1));

		// ConsumerConnector creates the message stream for each topic
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumerConnector
				.createMessageStreams(topicCount);

		// Get Kafka stream for topic 'my-input-topic'
		List<KafkaStream<byte[], byte[]>> kStreamList = consumerStreams.get(topicName);

		List<UserLocation> locations=Lists.newArrayList();
		
		// Iterate stream using ConsumerIterator
		for (final KafkaStream<byte[], byte[]> kStreams : kStreamList) {
			ConsumerIterator<byte[], byte[]> consumerIte = kStreams.iterator();
			while (consumerIte.hasNext() && locations.size()<50) {
				final MessageAndMetadata<byte[], byte[]> data = consumerIte.next();
				final String value = parseByteArray(data.message());
				final Long count = new Long(value);
				final String key = new String(data.key());
				locations.add(new UserLocation(key, count));
				System.out.println(
						"Message consumed from topic[" + topicName + "] : " + "location= " + key + "count= " + count);
			}
		}

		// Shutdown the consumer connector
		if (consumerConnector != null)
			consumerConnector.shutdown();
		
		return locations;
	}

	private static String parseByteArray(byte[] array) {
		String value = Arrays.toString(array);
		return new String(value.substring(1, value.length() - 1)
				.replaceAll(",", "").replaceAll("-", "").replaceAll("\\s+", "").trim());
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public List<UserLocation> consumeFromTopic(String topic) throws Exception {
		KafkaTwitterStreamConsumer consumer = new KafkaTwitterStreamConsumer();
		consumer.initialize();
		return consumer.consume(topic);
	}
}
