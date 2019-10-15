package com.myboot.dataprocess.process.kafka.producer;

import java.util.Map;

public interface MyKafkaProducerHistoryService {
	
    public void sendHisMessage(String topic,Map<String, Object> params);
}
