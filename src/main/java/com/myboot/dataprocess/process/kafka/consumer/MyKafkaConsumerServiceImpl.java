package com.myboot.dataprocess.process.kafka.consumer;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.myboot.dataprocess.model.KafkaApplyCardEntity;
import com.myboot.dataprocess.process.akka.MyAkkaProcessService;
import com.myboot.dataprocess.process.kafka.common.KafkaDataModelProcess;
import com.myboot.dataprocess.process.phoenix.MyPhoenixProcessService;
import com.myboot.dataprocess.tools.CommonTool;

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
/*@Scope("prototype")*/
public class MyKafkaConsumerServiceImpl  implements MyKafkaConsumerService {
	
	@Autowired
	private MyAkkaProcessService akkaService;
	
	@Autowired
	private MyPhoenixProcessService phoenixService;
	
	private static int sourceCount = 0 ; 
	
	private static int resultCount = 0 ; 
	
    @Autowired
    private KafkaDataModelProcess kafkaDataModelProcess;
	
	@KafkaListener(topics = {"${kafka.other.source.topic}"})
    public void listener(String content) {
    	log.info("==================MyKafkaCustermer listener start :"+(++sourceCount)+"=========================");
 		//log.info("MyKafkaConsumerServiceImpl#listener:receive "+(++sourceCount)+" source kafka message :" + content );
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
 				String myDataFlag = mapMessage.get("MyDataFlag")==null?"2":mapMessage.get("MyDataFlag").toString();
 				if("2".equals(myDataFlag)) {
 					 //log.info("save phoenix message success...");
 					 phoenixService.processPhoenix(mapMessage,0);
 				}
 				log.info("MyKafkaConsumerServiceImpl#listener:receive " +sourceCount+" source kafka message consume total cost :" + (System.currentTimeMillis() - start) +"ms");
 			}
 		}catch(Exception e) {
 			log.error(e.getMessage());
 		}
         log.info("==================MyKafkaCustermer listener end :"+ sourceCount +"========================");
    }
	
	@KafkaListener(topics = {"${kafka.other.dest.topic}"})
    public void listenerResult(String content) {
		//log.info("MyKafkaConsumerServiceImpl#listenerResult:receive "+(++resultCount)+" result topic kafka message :" + content );
		log.info("MyKafkaConsumerServiceImpl#listenerResult:receive "+(++resultCount)+" result topic kafka message " );
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


	@Override
	public void listenerTest(int myDataFlag, int count) {
		new Thread() {                    
		      public void run() { 
		    	String content = "";
		  		for(int i = 0; i<count;i++) {
		  			String currentDate = CommonTool.getCurrentDate();
		  			KafkaApplyCardEntity entity = kafkaDataModelProcess.assembleKafkaData(currentDate);
		          	Map<String,Object> mapMessage = new HashMap<String,Object>();
		          	mapMessage = entity.getData();
		  			if(myDataFlag == 1) {
		  				//历史造数
		  				mapMessage.put("MyStartDate","20190313");
		  		    	mapMessage.put("MyEndDate","20190929");
		  		    	mapMessage.put("MyDataFlag","1");
		  			}else {
		  				//实时
		  				//mapMessage.put("MyStartDate","");
		  		    	//mapMessage.put("MyEndDate","");
		  		    	//mapMessage.put("MyDataFlag","2");
		  		    	//mapMessage.put("FLAT_TRAD_DATE_TIME", "2019-10-13 00:00:00");
		  			}
		  			entity.setData(mapMessage);
		  			Gson gson = new Gson();
		  			content = gson.toJson(entity);
		  			listener(content);
		  		}                        
		      }
		    }.start(); 
	}
	
}