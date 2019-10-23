package com.myboot.dataprocess.process.akka;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.myboot.dataprocess.common.MyConstants;
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
			int myDataFlag = mapMessage.get(MyConstants.MY_DATA_FLAG_NAME)==null?2:Integer.valueOf(mapMessage.get(MyConstants.MY_DATA_FLAG_NAME).toString());
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
	    	mapMessage.put(MyConstants.MY_START_DATE_NAME,"");
	    	mapMessage.put(MyConstants.MY_END_DATE_NAME,"");
	    	mapMessage.put(MyConstants.MY_DATA_FLAG_NAME, MyConstants.MY_DATA_FLAG_REAL);
	    	mapMessage.put(MyConstants.FLAT_TRAD_DATE_TIME_NAME,MyConstants.FLAT_TRAD_DATE_TIME_DEFAULT);
	    	mapMessage.put(MyConstants.MY_APP_DATA_TYPE_NAME,MyConstants.MY_APP_DATA_TYPE_DEFAULT);
	    	String retJsonStr = "";
	    	if("HTTPCLIENT".equals(MyConstants.HTTP_TYPE)) {
	    		retJsonStr = MyHttpClientProcess.post(mapMessage);
	        }else {
	        	retJsonStr = AkkaHttpClient.post(mapMessage);
	        }
			log.info("MyAkkaProcessServiceImpl#processAkkaImpl:send real-time kafka message to akka cost :"+ (System.currentTimeMillis() - sendAkkaStart) +"ms");
			
			long sendKafkaStart = System.currentTimeMillis();
			Map<String,Object> retMap = MyHttpClientProcess.processResultContent(retJsonStr);
			if(retMap != null) {
				String applicationNumber = mapMessage.get("ApplicationNumber")==null?"ApplicationNumber":mapMessage.get("ApplicationNumber").toString();
				retMap.put("ApplicationNumber",applicationNumber);
	            String destTopic = myKafkaConfiguration.getOtherParameter("dest.topic");
	            kafkaService.sendResultMessage(destTopic,retMap);
			}
            log.info("MyAkkaProcessServiceImpl#processAkkaImpl:send akka message to result topic kafka cost :"+ (System.currentTimeMillis() - sendKafkaStart) +"ms" + retMap.toString());
            
    	}catch(Exception e) {
    		log.error("MyAkkaProcessServiceImpl#processAkkaImpl:send real-time kafka message to result topic kafka is fail...");
    		log.error(e.getMessage());
    	}
	}
	
	private void processHisAkkaImpl(Map<String, Object> mapMessage) {
		try {
	    	long sendAkkaStart = System.currentTimeMillis();
			mapMessage.put(MyConstants.MY_START_DATE_NAME,mapMessage.get(MyConstants.MY_START_DATE_NAME)==null?MyConstants.MY_START_DATE_DEFAULT:mapMessage.get(MyConstants.MY_START_DATE_NAME).toString());
	    	mapMessage.put(MyConstants.MY_END_DATE_NAME,mapMessage.get(MyConstants.MY_END_DATE_NAME)==null?MyConstants.MY_END_DATE_DEFAULT:mapMessage.get(MyConstants.MY_END_DATE_NAME).toString());
	    	mapMessage.put(MyConstants.MY_DATA_FLAG_NAME,MyConstants.MY_DATA_FLAG_HIS);
	    	String applicationDate = mapMessage.get(MyConstants.MY_APPLICATION_DATE_NAME)==null?MyConstants.MY_APPLICATION_DATE_DEFAULT:mapMessage.get(MyConstants.MY_APPLICATION_DATE_NAME).toString();
			mapMessage.put(MyConstants.FLAT_TRAD_DATE_TIME_NAME, applicationDate + " 00:00:00");
			mapMessage.put(MyConstants.MY_APP_DATA_TYPE_NAME,MyConstants.MY_APP_DATA_TYPE_DEFAULT);
	        if("HTTPCLIENT".equals(MyConstants.HTTP_TYPE)) {
	        	MyHttpClientProcess.post(mapMessage);
	        }else {
	        	AkkaHttpClient.post(mapMessage);
	        }
	        log.info("MyAkkaProcessServiceImpl#processHisAkkaImpl:send history kafka message to akka cost :"+ (System.currentTimeMillis() - sendAkkaStart) +"ms");
    	}catch(Exception e) {
    		log.error("MyAkkaProcessServiceImpl#processHisAkkaImpl:send history kafka message to akka is fail...");
    		log.error(e.getMessage());
    	}
	}

}
