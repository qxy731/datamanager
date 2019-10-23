package com.myboot.dataprocess.process.phoenix;

import java.util.HashMap;
import java.util.LinkedList;
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
	
	@ApiOperation(value="sql查询phoenix数据", notes="sql查询phoenix数据")
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
	
	@ApiOperation(value="保存", notes="保存")
	@RequestMapping(value = "/saveByColumn", method = {RequestMethod.POST,RequestMethod.GET})
	public StatusInfo<String> saveByColumn(@RequestBody Map<String,String> jsonStr) {
		StatusInfo<String> spi = null;
    	try {
    		service.save(jsonStr);
			spi = new StatusInfo<>();
    	}catch(Exception e) {
			spi = new StatusInfo<>(ErrorMessage.msg_opt_fail);
			log.info(ErrorMessage.msg_opt_fail.getMsg());
		}
    	return spi;
	}
	
	@ApiOperation(value="保存SQL", notes="保存SQL")
	@RequestMapping(value = "/saveBySql", method = {RequestMethod.POST,RequestMethod.GET})
	public StatusInfo<String> saveBySql(@RequestBody Map<String,LinkedList<String>> jsonStr) {
		StatusInfo<String> spi = null;
    	try {
    		List<String> sqls = jsonStr.get("sql")==null?new LinkedList<String>():(LinkedList<String>)jsonStr.get("sql");
    		service.save(sqls);
			spi = new StatusInfo<>();
    	}catch(Exception e) {
			spi = new StatusInfo<>(ErrorMessage.msg_opt_fail);
			log.info(ErrorMessage.msg_opt_fail.getMsg());
		}
    	return spi;
	}
	
	@ApiOperation(value="保存SQL文件数据", notes="保存SQL文件数据")
	@RequestMapping(value = "/write", method = {RequestMethod.POST,RequestMethod.GET})
	public StatusInfo<Map<String,Integer>> write(@RequestBody Map<String,LinkedList<String>> jsonStr) {
		StatusInfo<Map<String,Integer>> spi = null;
    	try {
    		Map<String,Integer> map = service.writeHisData();
			spi = new StatusInfo<Map<String,Integer>>(map);
    	}catch(Exception e) {
			spi = new StatusInfo<>(ErrorMessage.msg_opt_fail);
			log.info(ErrorMessage.msg_opt_fail.getMsg());
		}
    	return spi;
	}
	
	@ApiOperation(value="保存SQL文件数据", notes="保存SQL文件数据")
	@RequestMapping(value = "/writesql", method = {RequestMethod.POST,RequestMethod.GET})
	public StatusInfo<Map<String,Integer>> writeSQL(@RequestBody Map<String,LinkedList<String>> jsonStr) {
		StatusInfo<Map<String,Integer>> spi = null;
    	try {
    		List<String> sqls = jsonStr.get("sql")==null?new LinkedList<String>():(LinkedList<String>)jsonStr.get("sql");
    		Map<String,Integer> map = service.writeHisDataBySql(sqls);
			spi = new StatusInfo<Map<String,Integer>>(map);
    	}catch(Exception e) {
			spi = new StatusInfo<>(ErrorMessage.msg_opt_fail);
			log.info(ErrorMessage.msg_opt_fail.getMsg());
		}
    	return spi;
	}
	
	@ApiOperation(value="保存SQL文件数据", notes="保存SQL文件数据")
	@RequestMapping(value = "/writefile", method = {RequestMethod.POST,RequestMethod.GET})
	public StatusInfo<Map<String,Integer>> writeFile(@RequestBody Map<String,HashMap<String,String>> jsonStr) {
		StatusInfo<Map<String,Integer>> spi = null;
    	try {
    		Map<String,String> sqls = jsonStr.get("sql")==null?new HashMap<String,String>():(Map<String,String>)jsonStr.get("sql");
    		Map<String,Integer> map = service.writeHisDataByFile(sqls);
			spi = new StatusInfo<Map<String,Integer>>(map);
    	}catch(Exception e) {
			spi = new StatusInfo<>(ErrorMessage.msg_opt_fail);
			log.info(ErrorMessage.msg_opt_fail.getMsg());
		}
    	return spi;
	}
	
}