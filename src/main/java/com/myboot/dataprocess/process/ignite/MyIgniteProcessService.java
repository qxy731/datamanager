package com.myboot.dataprocess.process.ignite;

import java.util.List;
import java.util.Map;

public interface MyIgniteProcessService {
	
	public List<List<?>> query(String sql) throws Exception;
	
	public Map<String, Object> save(String[] fileNames) throws Exception;

}
