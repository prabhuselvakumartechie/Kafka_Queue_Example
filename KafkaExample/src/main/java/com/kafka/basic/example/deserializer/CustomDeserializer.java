/**
 * 
 */
package com.kafka.basic.example.deserializer;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.basic.example.pojo.CustomObject;

/**
 * @author prabhu.selvakumar
 *
 */
public class CustomDeserializer implements Deserializer<CustomObject> {

	final static Logger logger = Logger.getLogger(CustomDeserializer.class);

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public CustomObject deserialize(String topic, byte[] data) {
		ObjectMapper mapper = new ObjectMapper();
		CustomObject object = null;
		try {
			object = mapper.readValue(data, CustomObject.class);
		} catch (Exception exception) {
			System.out.println("Error in deserializing bytes " + exception);
			logger.info("Exception in deserializing bytes " + exception.getMessage());
		}
		return object;
	}

	@Override
	public void close() {
		logger.info("Deserialize Close Object");
	}
}
