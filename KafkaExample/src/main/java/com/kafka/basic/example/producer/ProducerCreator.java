/**
 * 
 */
package com.kafka.basic.example.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ser.std.NumberSerializers.LongSerializer;
import com.fasterxml.jackson.databind.ser.std.StringSerializer;
import com.kafka.basic.example.utils.KafkaConstants;

/**
 * @author prabhu.selvakumar
 *
 */
public class ProducerCreator {
	final static Logger logger = Logger.getLogger(ProducerCreator.class);
	
	public static Producer<Long, String> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.KAFKA_BROKERS_URL);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaConstants.CLIENT_ID);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		Producer<Long, String> kafkaProducer = new KafkaProducer<>(props);
		logger.info("Producer has been created");
		return kafkaProducer;
	}
}
