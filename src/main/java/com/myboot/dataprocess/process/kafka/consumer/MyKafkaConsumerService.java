package com.myboot.dataprocess.process.kafka.consumer;

import java.util.Map;

public interface MyKafkaConsumerService {
	public void listener(String content);
	
	public void processAkka(String rowkey,Map<String, Object> mapMessage);
	
	public void processPhoenix(String rowkey,Map<String, Object> mapMessage);
}
