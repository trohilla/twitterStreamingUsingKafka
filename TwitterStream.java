package com.tushar.kafka.twitter;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.common.collect.Lists;
import com.tushar.kafka.core.UserLocation;

public class TwitterStream {
	/**
	 * 
	 */
	private static List<UserLocation> locationsCount = Lists.newArrayList();
	
	/**
	 * @param args
	 * @throws Exception
	 */
	public void start(String topicName) throws Exception {

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

		// setting offset reset to earliest so that we can re-run the demo code
		// with the same pre-loaded data
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		// final Serde<String> stringSerde = Serdes.String();
		// final Serde<Long> longSerde = Serdes.Long();
		KStreamBuilder builder = new KStreamBuilder();
		KStream<String, String> source = builder.stream(topicName+"input");

		// KStream<String, Object> mapped=source.mapValues(record ->
		// Integer.valueOf(record.length()).toString());
		KTable<String,Long> counts = source.flatMapValues(TwitterStream::parseAndGetData)
				.filter((key,value)->(key!=null))
				.map((key, word) -> new KeyValue(word, word))
				.groupBy((k, v) -> k).count("Counts");
		counts.foreach(TwitterStream::populateLocations);

		// need to override value serde to Long type
		counts.to(Serdes.String(), Serdes.Long(), topicName);

		KafkaStreams streams = new KafkaStreams(builder, props);
		streams.start();

		// usually the stream application would be running forever,
		// in this example we just let it run for some time and stop since the
		// input data is finite.
		Thread.sleep(40000L);
		locationsCount.forEach(System.out::println);
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
		return;

	}

	/**
	 * @param json
	 * @return location
	 */
	private static List<String> parseAndGetData(String json) {
		final List<String> data = Lists.newArrayList();
		final JSONParser parser = new JSONParser();

		try {
			JSONObject object = (JSONObject) parser.parse(json);
			JSONObject user = (JSONObject) object.get("user");
			String location = (String) user.get("location");
			if (null != location && !"".equals(location))
				data.add(location);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return data;
	}

	private static List<UserLocation> populateLocations(String key, Long value) {
		UserLocation location = new UserLocation(key, value);
		locationsCount.add(location);
		return locationsCount;
	}
	
/*	public List<UserLocation> getUserLocations(String topicName) throws Exception
	{
		new TwitterStream().start(topicName);
		return locationsCount;
	}*/
}
