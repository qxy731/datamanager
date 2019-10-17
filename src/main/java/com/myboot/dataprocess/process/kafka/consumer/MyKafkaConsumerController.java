package com.myboot.dataprocess.process.kafka.consumer;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.myboot.dataprocess.common.ErrorMessage;
import com.myboot.dataprocess.common.StatusInfo;

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
public class MyKafkaConsumerController {
	
	@Autowired
	private MyKafkaConsumerService service;
	
	@ApiOperation(value="测试推送数据", notes="测试推送数据")
	@RequestMapping(value = "/consumer", method = {RequestMethod.POST,RequestMethod.GET})
	public StatusInfo<String> consumer(@RequestBody Map<String,String> map) {
		StatusInfo<String> spi = null;
    	try {
    		int count = map.get("count")==null?0:Integer.valueOf(map.get("count").toString());
    		int myDataFlag = map.get("myDataFlag")==null?1:Integer.valueOf(map.get("myDataFlag").toString());
    		service.listenerTest(myDataFlag,count);
    		spi = new StatusInfo<String>();
    	}catch(Exception e) {
			spi = new StatusInfo<>(ErrorMessage.msg_opt_fail);
			log.info(ErrorMessage.msg_opt_fail.getMsg());
		}
    	return spi ;
	}	
}