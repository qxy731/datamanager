package com.myboot.dataprocess.process.phoenix;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;

public class MyPhoenixProcessServiceImpl implements MyPhoenixProcessService{
	
	
	@Autowired
	private MyPhoenixProcessRepository repository;
	
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public List<Map<String, String>> query(String sql) throws Exception {
		return repository.query(sql);
	}

	@Override
	public void save(Map<String, String> map) throws Exception {
		repository.save(map);
	}

}
