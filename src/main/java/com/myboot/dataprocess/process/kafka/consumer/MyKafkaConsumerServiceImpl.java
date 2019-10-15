package com.myboot.dataprocess.process.kafka.consumer;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.myboot.dataprocess.process.akka.MyAkkaProcessService;
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
	
	@Autowired
	private MyAkkaProcessService akkaService;
	
	@Autowired
	private MyPhoenixProcessService phoenixService;
	
	@KafkaListener(topics = {"${kafka.other.source.topic}"})
    public void listener(String content) {
        log.info("==================MyKafkaCustermer listener start=========================");
		log.info(" message from kafka :" + content );
		try {
			if(content != null && content.length()>0) {
				long start = System.currentTimeMillis();
				Map<String, Object> mapMessage = processContent(content);
				if(mapMessage == null || mapMessage.size()==0 ) {
					return;
				}
				/**第一步：向akka传送数据并回写Kafka*/
				akkaService.processAkka(mapMessage);
				/**第二步：向phoenix保存数据*/
				String myDataType = mapMessage.get("MyDataType")==null?"2":mapMessage.get("MyDataType").toString();
				if("2".equals(myDataType)) {
					phoenixService.processPhoenix(mapMessage,0);
				}
				log.info("kafka consumer total cost :" + (System.currentTimeMillis() - start) +"ms");
			}
		}catch(Exception e) {
			log.error(e.getMessage());
		}
        log.info("==================MyKafkaCustermer listener end =========================");
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
	
}