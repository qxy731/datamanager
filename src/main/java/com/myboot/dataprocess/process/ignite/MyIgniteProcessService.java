package com.myboot.dataprocess.process.ignite;

import java.util.Map;

public interface MyIgniteProcessService {
	
	public Map<String,Object> query(Map<String,String> params) throws Exception;
	
	public Map<String, Object> save(String[] fileNames) throws Exception;

}
