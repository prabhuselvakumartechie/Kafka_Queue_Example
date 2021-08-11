/**
 * 
 */
package com.kafka.basic.example.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.Logger;

import com.kafka.basic.example.utils.KafkaConstants;

/**
 * @author prabhu.selvakumar
 *
 */
public class ConsumerObject {
	final static Logger logger = Logger.getLogger(ConsumerObject.class);
	
	public ConsumerObject() {
		logger.info("Consumer object has been created");
	}
	
	
	public void runConsumer() {
		Consumer<Long, String> consumer = ConsumerCreator.createConsumer();

		int noMessageToFetch = 0;

		while (true) {
			final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
			if (consumerRecords.count() == 0) {
				noMessageToFetch++;
				if (noMessageToFetch > KafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
					break;
				else
					continue;
			}

			consumerRecords.forEach(record -> {
				System.out.println("Record Key " + record.key());
				System.out.println("Record value " + record.value());
				System.out.println("Record partition " + record.partition());
				System.out.println("Record offset " + record.offset());

				logger.info("Record Key " + record.key());
				logger.info("Record value " + record.value());
				logger.info("Record partition " + record.partition());
				logger.info("Record offset " + record.offset());
			});

			consumer.commitAsync();
		}
		consumer.close();
	}
}
