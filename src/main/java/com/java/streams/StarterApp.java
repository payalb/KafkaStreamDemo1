package com.java.streams;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

public class StarterApp {
	// kstream is insert, ktable is update
	/*
	 * A stream in Kafka records the full history of world (or business) events from
	 * the beginning of time to today. It represents the past and the present. As we
	 * go from today to tomorrow, new events are constantly being added to the
	 * world’s history. This history is a sequence or “chain” of events, so you know
	 * which event happened before another event. A table in Kafka is the state of
	 * the world today. It represents the present. It is an aggregation of the
	 * history of world events, and this aggregation is changing constantly as we go
	 * from today to tomorrow.
	 */
	public static void main(String[] args) {
		Properties configuration = new Properties();
		configuration.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-demo1");
		configuration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		configuration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		configuration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		configuration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		// get stream from kafka
		/*
		 * In Kafka Streams you’d normally use StreamsBuilder#table() to read a Kafka
		 * topic into a KTable with a simple 1-liner:
		 * 
		 * KTable<String, String> table = builder.table("input-topic",
		 * Consumed.with(Serdes.String(), Serdes.String())); But, for the sake of
		 * illustration, you can also read the topic into a KStream first, and then
		 * perform the same aggregation step as shown above explicitly to turn the
		 * KStream into a KTable.
		 * 
		 * KStream<String, String> stream = ...; KTable<String, String> table = stream
		 * .groupByKey() .reduce((aggV, newV) -> newV);
		 */
		StreamsBuilder builder = new StreamsBuilder();
		/*
		 * In Kafka Streams you read a topic into a KStream via StreamsBuilder#stream().
		 * Here, you must define the desired schema via the Consumed.with() parameter
		 * for reading the topic’s data
		 */
		KStream<String, String> stream = builder.stream("topic1", Consumed.with(Serdes.String(), Serdes.String()));
		// convert values to lowercase
		KTable<String, Long> table = stream.mapValues(x -> x.toLowerCase())
				// flatmapvalues
				.flatMapValues(x -> Arrays.asList(x.split(" ")))
				.selectKey((x, y) -> y)
				.groupByKey()
				.count();

		// table.to(Serdes.String(), Serdes.Long(), "topic2");
		table.toStream().peek((x,y)-> System.out.println("**********"+x+":"+y)).to("topic2", Produced.with(Serdes.String(), Serdes.Long()));
		KafkaStreams s = new KafkaStreams(builder.build(), configuration);
		s.start();// starts the application
		System.out.println("Stream is" + s.toString());
		//Runtime.getRuntime().addShutdownHook(new Thread(s::close));

	}
}
