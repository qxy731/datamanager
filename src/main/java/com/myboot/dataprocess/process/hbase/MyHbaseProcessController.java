package com.myboot.dataprocess.process.hbase;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

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
* @ClassName ：HbaseProcessController 
* @Description ： 控制类
* @author ：PeterQi
*
*/
@Api
@Slf4j
@RestController
@RequestMapping("/hbase")
public class MyHbaseProcessController {
	
	@Autowired
	private MyHbaseProcessService service;
	
	@ApiOperation(value="HBase造数据", notes="HBase造数据")
	@RequestMapping(value = "/assemble", method = {RequestMethod.POST,RequestMethod.GET})
	public StatusInfo<String> assemble(@RequestBody Map<String,String> map) {
		StatusInfo<String> spi = null;
    	try {
    		String startDate = map.get("startDate")==null?"2019-10-13":map.get("startDate").toString();
    		String endDate = map.get("endDate")==null?"2019-10-13":map.get("endDate").toString();
    		int total = map.get("total")==null?0:Integer.valueOf(map.get("total").toString());
    		/**直接put到hbase**/
			service.save(total,startDate,endDate);
			spi = new StatusInfo<String>();
    	}catch(Exception e) {
			spi = new StatusInfo<>(ErrorMessage.msg_opt_fail);
			log.info(ErrorMessage.msg_opt_fail.getMsg());
		}
    	return spi ;
	}
	
	@ApiOperation(value="HBase历史数据预处理", notes="HBase历史数据预处理")
	@RequestMapping(value = "/process", method = {RequestMethod.POST,RequestMethod.GET})
	public StatusInfo<String> process(HttpServletRequest request) {
		StatusInfo<String> spi = null;
    	try {
			service.process();
			spi = new StatusInfo<String>();
    	}catch(Exception e) {
			spi = new StatusInfo<>(ErrorMessage.msg_opt_fail);
			log.info(ErrorMessage.msg_opt_fail.getMsg());
		}
    	return spi ;
	}
	
}