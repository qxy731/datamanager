package com.myboot.dataprocess.process.kafka.producer;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.myboot.dataprocess.common.ErrorMessage;
import com.myboot.dataprocess.common.MyConstants;
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
	@Qualifier("myKafkaProducerPhoenixServiceImpl") 
	private MyKafkaProducerHistoryService hisService;
	
	@Autowired
	private MyKafkaConfiguration myKafkaConfiguration;
	
	@ApiOperation(value="Kafka推送数据", notes="Kafka推送数据")
	@RequestMapping(value = "/assemble", method = {RequestMethod.POST,RequestMethod.GET})
	public StatusInfo<String> sendAssembleData(@RequestBody Map<String,String> map) {
		StatusInfo<String> spi = null;
    	try {
    		int count = map.get("count")==null?0:Integer.valueOf(map.get("count").toString());
    		String currentDate = CommonTool.getCurrentDate();
    		String topic =   myKafkaConfiguration.getOtherParameter("source.topic");
    		service.sendAssembleMessage(topic, count, currentDate);
    		spi = new StatusInfo<String>();
    	}catch(Exception e) {
			spi = new StatusInfo<>(ErrorMessage.msg_opt_fail);
			log.info(ErrorMessage.msg_opt_fail.getMsg());
		}
    	return spi ;
	}
	
	@ApiOperation(value="Kafka推送数据", notes="Kafka推送数据")
	@RequestMapping(value = "/history", method = {RequestMethod.POST,RequestMethod.GET})
	public StatusInfo<String> sendHisData(@RequestBody Map<String,String> jsonStr) {
		StatusInfo<String> spi = null;
    	try {
    		String topic = myKafkaConfiguration.getOtherParameter("source.topic");
    		Map<String,Object> params = new HashMap<String,Object>();
    		params.put(MyConstants.MY_START_DATE_NAME,jsonStr.get(MyConstants.MY_START_DATE_NAME));
    		params.put(MyConstants.MY_END_DATE_NAME,jsonStr.get(MyConstants.MY_END_DATE_NAME));
    		params.put(MyConstants.MY_DATA_FLAG_NAME,jsonStr.get(MyConstants.MY_DATA_FLAG_NAME));
    		hisService.sendHisMessage(topic,params);
    		spi = new StatusInfo<String>();
    	}catch(Exception e) {
			spi = new StatusInfo<>(ErrorMessage.msg_opt_fail);
			log.info(ErrorMessage.msg_opt_fail.getMsg());
		}
    	return spi ;
	}
	
}