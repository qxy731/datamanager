package com.myboot.dataprocess.process.kafka.consumer;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.myboot.dataprocess.process.httpclient.MyHttpClientProcess;
import com.myboot.dataprocess.process.kafka.common.MyKafkaConfiguration;
import com.myboot.dataprocess.process.kafka.producer.MyKafkaProducerService;
import com.myboot.dataprocess.process.phoenix.MyPhoenixProcessService;

import lombok.extern.slf4j.Slf4j;

/** 
*
* @ClassName ：MyKafkaCustermer 
* @Description ： 消费者处理类
* @author ：PeterQi
*
*/
@Slf4j
@Service
public class MyKafkaConsumerServiceImpl  implements MyKafkaConsumerService {
	
	private static long count = 0;
	
	@Autowired
	private MyKafkaConfiguration myKafkaConfiguration;
	
	@Autowired
	private MyKafkaProducerService kafkaService;
	
	@Autowired
	private MyPhoenixProcessService phoenixService;
	
	/*@KafkaListener(topics = {"${kafka.other.source.topic}"})*/
    public void listener(String content) {
        //log.info("==================MyKafkaCustermer listener start=========================");
		log.info("the" + (count++) + " message from kafka :" + content );
		try {
			if(content != null && content.length()>0 ) {
				long start = System.currentTimeMillis();
				Map<String, Object> mapMessage = processContent(content);
				if(mapMessage == null) {
					return;
				}
				String rowkey = createRowkeyFromKafka(mapMessage);
				if(rowkey == null) {
					return;
				}
				/**第一步：向akka传送数据并回写Kafka*/
				processAkka(rowkey,mapMessage);
				/**第二步：向phoenix保存数据*/
				processPhoenix(rowkey,mapMessage);
				long end = System.currentTimeMillis();
				long mins = start - end;
				log.info("kafka consumer total cost :" + mins +"ms");
			}
		}catch(Exception e) {
			log.error(e.getMessage());
		}
        //log.info("==================MyKafkaCustermer listener end =========================");
    }
	
	/**
	 * 把接收的消息转成Map
	 * @param content
	 * @return
	 */
	private Map<String, Object> processContent(String content){
		Gson gson = new Gson();
		Map<String,Object> map = gson.fromJson(content, new TypeToken<HashMap<String,Object>>(){}.getType());
        if(map==null) {
        	return null;
        }
        @SuppressWarnings("unchecked")
		Map<String,Object> dataObject = (Map<String,Object>)map.get("data");
		return dataObject;
	}
	
	/**
     * 根据数据构建rowkey
     * @return
     */
    public String createRowkeyFromKafka(Map<String,Object> mapMessage) {
        return mapMessage.get("ApplicationNumber")==null?"ApplicationNumber":mapMessage.get("ApplicationNumber").toString();
    }
    
    /**
     * 向akka传送数据
     * @throws Exception 
     */
    public void processAkka(String rowkey,Map<String, Object> mapMessage) {
    	try {
	    	long sendAkkaStart = System.currentTimeMillis();
	        //String akkaApi = myKafkaConfiguration.getOtherParameter("akka_api");
			//AkkaProcess akkaProcess = AkkaProcess.getInstance();
			//Map<String, Map<String, Object>> dtoResult = akkaProcess.getResultsFormEvalResultDto(mapMessage, akkaApi, rowkey); 
			
	        String retJsonStr = MyHttpClientProcess.post(mapMessage);
	        Map<String,Object> retMap = processContent(retJsonStr);
	        //dtoResult.
			long sendAkkaEnd = System.currentTimeMillis();
			long minsAkka = sendAkkaEnd - sendAkkaStart;
			log.info("send source message to akka cost :"+ minsAkka +"ms");
			long sendKafkaStart = System.currentTimeMillis();
			retMap.put("ApplicationNumber",rowkey);
            String destTopic = myKafkaConfiguration.getOtherParameter("dest.topic");
            kafkaService.sendResultMessage(destTopic,retMap);
			/*for(Map<String,Object> results : dtoResult.values()) {
	            //将原来的数据一起放入
	            results.putAll(mapMessage);
	            String destTopic = myKafkaConfiguration.getOtherParameter("dest.topic");
	            kafkaService.sendResultMessage(destTopic,results);
	        }*/
			long sendKafkaEnd = System.currentTimeMillis();
			long minsKafka = sendKafkaEnd - sendKafkaStart;
			log.info("send result message to kafka cost :"+ minsKafka +"ms");
    	}catch(Exception e) {
    		log.error(" send source message to kafka is fail...");
    		log.error(e.getMessage());
    	}
    }
    
    /**
     * 向phoenix保存数据
     * @throws Exception 
     */
    public void processPhoenix(String rowkey,Map<String, Object> mapMessage) {
    	try {
	    	long sendPhoenixStart = System.currentTimeMillis();
	    	Map<String,String> map = new LinkedHashMap<String,String>();
			for(Map.Entry<String,Object> entry : mapMessage.entrySet()) {
			String key = entry.getKey();
		    Object value = entry.getValue();
		    String cvalue = "";
		    if(value != null) {
			     cvalue = String.valueOf(value);
			}
		    map.put(key.toUpperCase(), cvalue);
		}
		phoenixService.save(map);
		long sendPhoenixEnd = System.currentTimeMillis();
		long mins = sendPhoenixEnd - sendPhoenixStart;
		log.info(" send source message to phoenix cost :" + mins + "ms");
    	}catch(Exception e) {
    		log.error(" send source message to phoenix is fail...");
    		log.error(e.getMessage());
    	}
    }
    
    


}