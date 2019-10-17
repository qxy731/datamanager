package com.myboot.dataprocess.process.kafka.consumer;

public interface MyKafkaConsumerService {
	
	public void listener(String content);
	
	public void listenerTest(int myDataFlag,int count);
	
	public void listenerResult(String content);
	
}
