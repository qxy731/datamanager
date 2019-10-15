package com.myboot.dataprocess.process.kafka.producer;

import java.util.Map;

public interface MyKafkaProducerService {
	
	public void sendAssembleMessage(String topic, int count,String currentDate) throws Exception;
	
	public void sendResultMessage(String topic,Map<String, Object> mapMessage) throws Exception;
	
}
