package com.myboot.dataprocess.kafka;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.google.gson.Gson;
import com.myboot.dataprocess.DataprocessApplication;
import com.myboot.dataprocess.builder.RandomDataModelBuilder;
import com.myboot.dataprocess.model.KafkaApplyCardEntity;
import com.myboot.dataprocess.process.kafka.common.KafkaDataModelProcess;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = DataprocessApplication.class)
public class KafkaDataModelProcessTest {
	
    @Autowired
    private KafkaDataModelProcess kafkaDataModelProcess;
	
	@Test
    public void testSendMessage() throws Exception {
		Map<String,Object> map = RandomDataModelBuilder.getRandomDataModel(1234566L, "2019-10-10");
		System.out.println(map);
		Map<String,Object> retMap = new LinkedHashMap<String,Object>();
		for(Map.Entry<String, Object> ss : map.entrySet()) {
			String key = ss.getKey();
			Object value = ss.getValue();
			retMap.put(key.toUpperCase(), value);
			//map.remove(key);
		}
		retMap.put("MyDataType", "1");
		Gson gson = new Gson();
		String json = gson.toJson(retMap);
		log.info("retMap message :" + json);
		KafkaApplyCardEntity entity = kafkaDataModelProcess.processHisKafkaData(map);
    	String jsonStr = gson.toJson(entity);
    	log.info("send phoenix his data message :" + jsonStr);
	}
	

}
