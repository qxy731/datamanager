package com.myboot.dataprocess.process.phoenix;

import java.util.List;
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
* @ClassName ：MyPhoenixProcessController 
* @Description ： 控制类
* @author ：PeterQi
*
*/
@Api
@Slf4j
@RestController
@RequestMapping("/phoenix")
public class MyPhoenixProcessController {
	
	@Autowired
	private MyPhoenixProcessService service;
	
	@ApiOperation(value="sql查询", notes="sql查询")
	@RequestMapping(value = "/query", method = {RequestMethod.POST,RequestMethod.GET})
	public StatusInfo<List<Map<String, String>>> query(@RequestBody Map<String,String> jsonStr) {
		StatusInfo<List<Map<String, String>>> spi = null;
    	try {
    		String sql = jsonStr.get("sql")==null?"":jsonStr.get("sql").toString();
    		List<Map<String, String>> list = service.query(sql);
			spi = new StatusInfo<List<Map<String, String>>>(list);
    	}catch(Exception e) {
			spi = new StatusInfo<>(ErrorMessage.msg_opt_fail);
			log.info(ErrorMessage.msg_opt_fail.getMsg());
		}
    	return spi;
	}

}