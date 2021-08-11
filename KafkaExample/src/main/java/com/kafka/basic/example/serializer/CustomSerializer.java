/**
 * 
 */
package com.kafka.basic.example.serializer;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.basic.example.pojo.CustomObject;

/**
 * @author prabhu.selvakumar
 *
 */
public class CustomSerializer implements Serializer<CustomObject> {
	final static Logger logger = Logger.getLogger(CustomSerializer.class);

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub

	}

	@Override
	public byte[] serialize(String topic, CustomObject data) {
		// TODO Auto-generated method stub
		byte[] retVal = null;
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			retVal = objectMapper.writeValueAsString(data).getBytes();
		} catch (Exception exception) {
			System.out.println("Error in serializing object" + data);
			logger.info("Exception in serializing object >>> " + data);
			logger.info("Exception while serializing object >>> " + exception.getMessage());
		}
		return retVal;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
