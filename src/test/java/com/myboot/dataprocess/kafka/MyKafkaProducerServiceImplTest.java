package com.myboot.dataprocess.kafka;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.myboot.dataprocess.DataprocessApplication;
import com.myboot.dataprocess.process.kafka.common.MyKafkaConfiguration;
import com.myboot.dataprocess.process.kafka.producer.MyKafkaProducerServiceImpl;


@RunWith(SpringRunner.class)
@SpringBootTest(classes = DataprocessApplication.class)
public class MyKafkaProducerServiceImplTest {
	
	@Autowired
    private MyKafkaProducerServiceImpl service;
	
	@Autowired
	private MyKafkaConfiguration myKafkaConfiguration;
	
	@Test
    public void testSendMessage() throws Exception {
		int count = 100;
		String currentDate = "2019-10-08";
		String topic = myKafkaConfiguration.getOtherParameter("topic");
		service.sendAssembleMessage(topic, count, currentDate);
	}
	

}
