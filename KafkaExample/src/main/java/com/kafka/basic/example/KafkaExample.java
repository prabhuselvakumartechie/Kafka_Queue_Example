/**
 * 
 */
package com.kafka.basic.example;

import org.apache.log4j.Logger;

import com.kafka.basic.example.consumer.ConsumerObject;
import com.kafka.basic.example.producer.ProducerObject;

/**
 * @author prabhu.selvakumar
 *
 */
public class KafkaExample {
	
	final static Logger logger = Logger.getLogger(KafkaExample.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		ProducerObject aProducer = new ProducerObject();
		aProducer.runProducer();
		
//		try {
//			Thread.sleep(300000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//		
//		ConsumerObject aConsumer = new ConsumerObject();
//		aConsumer.runConsumer();
		
	}
	
	

}
