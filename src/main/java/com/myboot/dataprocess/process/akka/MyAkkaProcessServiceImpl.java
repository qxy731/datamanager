package com.myboot.dataprocess.process.akka;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.myboot.dataprocess.process.httpclient.MyHttpClientProcess;
import com.myboot.dataprocess.process.kafka.common.MyKafkaConfiguration;
import com.myboot.dataprocess.process.kafka.producer.MyKafkaProducerService;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class MyAkkaProcessServiceImpl implements MyAkkaProcessService{

	ExecutorService E1 = Executors.newSingleThreadExecutor();
	
	@Autowired
	private MyKafkaConfiguration myKafkaConfiguration;
	
	@Autowired
	private MyKafkaProducerService kafkaService;
	
	/**
     * 向akka传送数据
     * @throws Exception 
     */
	@Override
	public void processAkka(Map<String, Object> mapMessage) {
		E1.execute(() -> {
			try {
				if(mapMessage != null) {
					processAkkaImpl(mapMessage);
				}
			}catch(Exception e) {
	    		log.error(" send source message to akka is fail...");
	    		log.error(e.getMessage());
	    	}
    	});
	}
	
	private void processAkkaImpl(Map<String, Object> mapMessage) {
		try {
	    	long sendAkkaStart = System.currentTimeMillis();
	        String retJsonStr = MyHttpClientProcess.post(mapMessage);
	        Map<String,Object> retMap = MyHttpClientProcess.processResultContent(retJsonStr);
			long sendAkkaEnd = System.currentTimeMillis();
			long minsAkka = sendAkkaEnd - sendAkkaStart;
			log.info("send source message to akka cost :"+ minsAkka +"ms");
			
			long sendKafkaStart = System.currentTimeMillis();
			String applicationNumber = mapMessage.get("ApplicationNumber")==null?"ApplicationNumber":mapMessage.get("ApplicationNumber").toString();
			retMap.put("ApplicationNumber",applicationNumber);
            String destTopic = myKafkaConfiguration.getOtherParameter("dest.topic");
            kafkaService.sendResultMessage(destTopic,retMap);
			long sendKafkaEnd = System.currentTimeMillis();
			long minsKafka = sendKafkaEnd - sendKafkaStart;
			log.info("send result message to kafka cost :"+ minsKafka +"ms");
			log.info(retMap.toString());
			
    	}catch(Exception e) {
    		log.error(" send source message to kafka is fail...");
    		log.error(e.getMessage());
    	}
	}

}
