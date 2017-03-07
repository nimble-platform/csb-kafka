package com.csb.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.CSB.Kafka.CSBProducer;

public class ProducerTest {

	public static void main(String[] args) {
		CSBProducer csbProducer = new CSBProducer();
		csbProducer.sendMsg("first", "my-topic2");
		csbProducer.close();
		/*
		 * Properties props = new Properties(); props.put("bootstrap.servers",
		 * "localhost:9092"); props.put("acks", "all"); props.put("retries", 0);
		 * props.put("batch.size", 16384); props.put("linger.ms", 1);
		 * props.put("buffer.memory", 33554432); props.put("key.serializer",
		 * "org.apache.kafka.common.serialization.StringSerializer");
		 * props.put("value.serializer",
		 * "org.apache.kafka.common.serialization.StringSerializer");
		 * Producer<String, String> producer = null; try { producer = new
		 * KafkaProducer<>(props); for (int i = 0; i < 100; i++) { String msg =
		 * "Message " + i; producer.send(new ProducerRecord<String,
		 * String>("my-topic2", msg)); System.out.println("Sent:" + msg); } }
		 * catch (Exception e) { e.printStackTrace();
		 * 
		 * } finally { producer.close(); }
		 */
	}

}
