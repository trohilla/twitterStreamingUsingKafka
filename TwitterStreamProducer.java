/**
 * Copyright 2013 Twitter, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.tushar.kafka.twitter;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterStreamProducer {

	public void run(String topicName,String filter)
			throws InterruptedException {
		
		String consumerKey = "7OOHTBQOCmxVEdvwlpeX34DZP";
		String consumerSecret = "q4B5gV4MkWyykNSbpMspd5jTRAxf1DaceNJ32ENQknUGjNJMA9";
		String token = "260644023-aDKdyhJrxyYBw35cDW34Z61svXwNQ2s1oCwuEp0U";
		String secret = "uZOTl81gmkeIoxVzkiH6zzH77GwIWYtQN16XnorPQXw7Y";
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		// add some track terms
		endpoint.trackTerms(Lists.newArrayList(filter));

		Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
		// Authentication auth = new BasicAuth(username, password);

		// Create a new BasicClient. By default gzip is enabled.
		Client client = new ClientBuilder().hosts(Constants.STREAM_HOST).endpoint(endpoint).authentication(auth)
				.processor(new StringDelimitedProcessor(queue)).build();

		// Establish a connection
		client.connect();

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		// Do whatever needs to be done with messages
		for (int msgRead = 0; msgRead < 10; msgRead++) {
			String msg = queue.take();
			System.out.println(msgRead + "  " + msg);
			producer.send(new ProducerRecord<String, String>(topicName, String.valueOf(msgRead), msg));
		}
		
		producer.close();
		client.stop();

	}
}
