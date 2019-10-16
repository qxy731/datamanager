package com.myboot.dataprocess.process.akka;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.myboot.dataprocess.process.httpclient.MyHttpClientProcess;
import com.myboot.dataprocess.process.kafka.common.MyKafkaConfiguration;
import com.myboot.dataprocess.process.kafka.producer.MyKafkaProducerService;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class MyAkkaProcessServiceImpl implements MyAkkaProcessService{

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
		if(mapMessage != null) {
			int myDataFlag = mapMessage.get("MyDataFlag")==null?2:Integer.valueOf(mapMessage.get("MyDataFlag").toString());
			if(myDataFlag == 2) {
				processAkkaImpl(mapMessage);
			}else {
				processHisAkkaImpl(mapMessage);
			}
		}
	}
	
	private void processAkkaImpl(Map<String, Object> mapMessage) {
		try {
	    	long sendAkkaStart = System.currentTimeMillis();
	    	mapMessage.put("MyStartDate", "");
	    	mapMessage.put("MyEndDate", "");
	    	mapMessage.put("MyDataFlag", "2");
	    	mapMessage.put("FLAT_TRAD_DATE_TIME","2019-10-13 00:00:00");
	        String retJsonStr = MyHttpClientProcess.post(mapMessage);
			log.info("send kafka message to akka cost :"+ (System.currentTimeMillis() - sendAkkaStart) +"ms");
			
			long sendKafkaStart = System.currentTimeMillis();
			Map<String,Object> retMap = MyHttpClientProcess.processResultContent(retJsonStr);
			String applicationNumber = mapMessage.get("ApplicationNumber")==null?"ApplicationNumber":mapMessage.get("ApplicationNumber").toString();
			retMap.put("ApplicationNumber",applicationNumber);
            String destTopic = myKafkaConfiguration.getOtherParameter("dest.topic");
            kafkaService.sendResultMessage(destTopic,retMap);
            log.info("send akka message to kafka cost :"+ (System.currentTimeMillis() - sendKafkaStart) +"ms" + retMap.toString());
            
    	}catch(Exception e) {
    		log.error(" send source message to akka is fail...");
    		log.error(e.getMessage());
    	}
	}
	
	private void processHisAkkaImpl(Map<String, Object> mapMessage) {
		try {
	    	long sendAkkaStart = System.currentTimeMillis();
			String applicationDate = mapMessage.get("ApplicationDate")==null?"2019-10-13":mapMessage.get("ApplicationDate").toString();
			mapMessage.put("FLAT_TRAD_DATE_TIME", applicationDate + " 00:00:00");
	        MyHttpClientProcess.post(mapMessage);
	        log.info("send kafka message to akka cost :"+ (System.currentTimeMillis() - sendAkkaStart) +"ms");
    	}catch(Exception e) {
    		log.error(" send source message to akka is fail...");
    		log.error(e.getMessage());
    	}
	}

}
