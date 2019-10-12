package com.myboot.dataprocess.kafka;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.myboot.dataprocess.DataprocessApplication;
import com.myboot.dataprocess.process.kafka.consumer.MyKafkaConsumerServiceImpl;


@RunWith(SpringRunner.class)
@SpringBootTest(classes = DataprocessApplication.class)
public class MyKafkaConsumerServiceImplTest {
	
	@Autowired
    private MyKafkaConsumerServiceImpl service;
	
	/*@Autowired
	private MyKafkaConfiguration myKafkaConfiguration;*/
	
	@Test
    public void testProcessPhoenix() throws Exception {
		String rowkey = "12345678901";
		Map<String, Object> mapMessage = new LinkedHashMap<String,Object>();
		mapMessage.put("ApplicationNumber", rowkey);
		mapMessage.put("AlarmCode", "H");
		service.processPhoenix(rowkey, mapMessage);
	}
	

}
