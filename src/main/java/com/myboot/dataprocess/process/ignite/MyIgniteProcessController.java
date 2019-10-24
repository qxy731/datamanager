package com.myboot.dataprocess.process.ignite;

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
* @ClassName ：MyIgniteProcessController 
* @Description ： 控制类
* @author ：PeterQi
*
*/
@Api
@Slf4j
@RestController
@RequestMapping("/ignite")
public class MyIgniteProcessController {
	
	
	@Autowired
	private MyIgniteProcessService service;
	
	@ApiOperation(value="sql查询ignite数据", notes="sql查询ignite数据")
	@RequestMapping(value = "/query", method = {RequestMethod.POST,RequestMethod.GET})
	public StatusInfo<Map<String,Object>> query(@RequestBody Map<String,String> jsonStr) {
		StatusInfo<Map<String,Object>> spi = null;
    	try {
    		//String sql = jsonStr.get("fileNo")==null?"":jsonStr.get("sql").toString();
    		Map<String,Object> map = service.query(jsonStr);
			spi = new StatusInfo<Map<String,Object>>(map);
    	}catch(Exception e) {
			spi = new StatusInfo<>(ErrorMessage.msg_opt_fail);
			log.info(ErrorMessage.msg_opt_fail.getMsg());
			e.printStackTrace();
		}
    	return spi;
	}
	
	@ApiOperation(value="保存", notes="保存")
	@RequestMapping(value = "/save", method = {RequestMethod.POST,RequestMethod.GET})
	public StatusInfo<Map<String, Object>> save(@RequestBody Map<String,String> jsonStr) {
		StatusInfo<Map<String, Object>> spi = null;
    	try {
    		String fileNames = jsonStr.get("fileNames")==null?"":jsonStr.get("fileNames").toString();
    		String[] fileNameArr = null;
    		if(!fileNames.equals("")&&fileNames.length()>0) {
    			fileNameArr = fileNames.split(",");
    		}
    		Map<String, Object> map = service.save(fileNameArr);
    		spi = new StatusInfo<Map<String, Object>>(map);
    	}catch(Exception e) {
			spi = new StatusInfo<>(ErrorMessage.msg_opt_fail);
			log.info(ErrorMessage.msg_opt_fail.getMsg());
			e.printStackTrace();
		}
    	return spi;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
