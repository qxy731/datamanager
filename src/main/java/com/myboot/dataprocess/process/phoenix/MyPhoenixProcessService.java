package com.myboot.dataprocess.process.phoenix;

import java.util.List;
import java.util.Map;

public interface MyPhoenixProcessService {
	
	public List<Map<String,String>> query(String sql) throws Exception;
	
	public void processPhoenix(Map<String, Object> mapMessage,int type);
	
	public void save(Map<String,String> map) throws Exception;
	
	public void save(List<String> sqls) throws Exception;
	
	public Map<String,Integer> writeHisData()  throws Exception;
	
	public Map<String,Integer> writeHisDataBySql(List<String> sqls)  throws Exception;
	
	public Map<String,Integer> writeHisDataByFile(Map<String,String> map)  throws Exception;
	
}