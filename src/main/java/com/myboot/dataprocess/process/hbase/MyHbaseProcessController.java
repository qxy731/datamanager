package com.myboot.dataprocess.process.hbase;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
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
	public StatusInfo<String> assemble(HttpServletRequest request) {
		StatusInfo<String> spi = null;
    	try {
    		spi = new StatusInfo<String>();
    		String startDate = request.getParameter("startDate");
    		String endDate = request.getParameter("endDate");
    		int total = Integer.valueOf(request.getParameter("total"));
    		/**直接put到hbase**/
			service.save(total,startDate,endDate);
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
    		spi = new StatusInfo<String>();
			service.process();
    	}catch(Exception e) {
			spi = new StatusInfo<>(ErrorMessage.msg_opt_fail);
			log.info(ErrorMessage.msg_opt_fail.getMsg());
		}
    	return spi ;
	}
	
}