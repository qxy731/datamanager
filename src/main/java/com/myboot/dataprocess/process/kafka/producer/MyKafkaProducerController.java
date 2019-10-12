package com.myboot.dataprocess.process.kafka.producer;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.myboot.dataprocess.common.ErrorMessage;
import com.myboot.dataprocess.common.StatusInfo;
import com.myboot.dataprocess.process.kafka.common.MyKafkaConfiguration;
import com.myboot.dataprocess.tools.CommonTool;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;


/** 
*
* @ClassName ：MyKafkaProducerController 
* @Description ： 控制类
* @author ：PeterQi
*
*/
@Api
@Slf4j
@RestController
@RequestMapping("/kafka")
public class MyKafkaProducerController {
	
	@Autowired
	private MyKafkaProducerService service;
	
	@Autowired
	private MyKafkaConfiguration myKafkaConfiguration;
	
	@ApiOperation(value="Kafka推送数据", notes="Kafka推送数据")
	@RequestMapping(value = "/assemble", method = {RequestMethod.POST,RequestMethod.GET})
	public StatusInfo<String> assemble(@RequestBody Map<String,String> map) {
		StatusInfo<String> spi = null;
    	try {
    		int count = map.get("count")==null?0:Integer.valueOf(map.get("count").toString());
    		String currentDate = CommonTool.getCurrentDate();
    		String topic = myKafkaConfiguration.getOtherParameter("source.topic");
    		service.sendMessage(topic, count, currentDate);
    		spi = new StatusInfo<String>();
    	}catch(Exception e) {
			spi = new StatusInfo<>(ErrorMessage.msg_opt_fail);
			log.info(ErrorMessage.msg_opt_fail.getMsg());
		}
    	return spi ;
	}
	
}