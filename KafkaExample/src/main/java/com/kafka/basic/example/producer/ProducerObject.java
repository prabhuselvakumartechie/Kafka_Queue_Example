/**
 * 
 */
package com.kafka.basic.example.producer;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import com.kafka.basic.example.utils.KafkaConstants;

/**
 * @author prabhu.selvakumar
 *
 */
public class ProducerObject {

	final static Logger logger = Logger.getLogger(ProducerObject.class);

	public ProducerObject() {
		logger.info("Producer object has been created");
	}

	public void runProducer() {
		Producer<Long, String> aProducer = ProducerCreator.createProducer();

		for (int index = 0; index < KafkaConstants.MESSAGE_COUNT; index++) {
			final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(KafkaConstants.TOPIC_NAME,
					"This is record " + index);
			try {
				RecordMetadata metadata = aProducer.send(record).get();
				System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
						+ " with offset " + metadata.offset());
				logger.info("Record sent with key " + index + " to partition " + metadata.partition() + " with offset "
						+ metadata.offset());
			} catch (ExecutionException e) {
				System.out.println("Execution Error while sending record");
				System.out.println(e);
				logger.info("Execution Error while sending record >>>" +e.getMessage());
			} catch (InterruptedException e) {
				System.out.println("Interrupted Error while sending record");
				System.out.println(e);
				logger.info("Interrupted Error while sending record >>>" +e.getMessage());
			}
		}
	}

}
