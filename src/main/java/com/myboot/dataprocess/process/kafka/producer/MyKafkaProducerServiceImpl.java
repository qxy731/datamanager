package com.myboot.dataprocess.process.kafka.producer;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.myboot.dataprocess.model.KafkaApplyCardEntity;
import com.myboot.dataprocess.process.kafka.common.KafkaDataModelProcess;

import lombok.extern.slf4j.Slf4j;

/** 
*
* @ClassName ：MyKafkaProducer 
* @Description ：生产者处理类
* @author ：PeterQi
*
*/
@Slf4j
@Service
public class MyKafkaProducerServiceImpl implements MyKafkaProducerService {
	
    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;
    
    @Autowired
    private KafkaDataModelProcess kafkaDataModelProcess;
    
    /**
     * 发送造数数据
     */
    public void sendAssembleMessage(String topic, int count,String currentDate) {
        if(null == topic) {
            return;
        }
        for(int i=0;i<count;i++) {
        	KafkaApplyCardEntity entity = kafkaDataModelProcess.assembleKafkaData(currentDate);
        	Gson gson = new Gson();
        	String jsonStr = gson.toJson(entity);
        	//log.info("send assemble data message :" + jsonStr);
        	log.info("send assemble data message :" + (i+1));
            kafkaTemplate.send(topic,jsonStr);
        }
    }
    
    /**
     * 发送历史库表数据
     */
    public void sendHisMessage(String topic, Map<String, Object> entity) {
    	Gson gson = new Gson();
    	String jsonStr = gson.toJson(entity);
    	//log.info("send his data message :" + jsonStr);
        kafkaTemplate.send(topic,jsonStr);
    }
    
    /**
     * 发送衍生变量计算结果数据
     */
    public void sendResultMessage(String topic,Map<String, Object> entity) {
    	Gson gson = new Gson();
    	String jsonStr = gson.toJson(entity);
    	//log.info("send result message :" + jsonStr);
        kafkaTemplate.send(topic,jsonStr);
    }

}