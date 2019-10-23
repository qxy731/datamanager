package com.myboot.dataprocess.process.ignite;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;

import com.google.gson.Gson;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MyIgniteProcess {
	
	/**
	  * 读取文件中的数据存入ignite
	  * @param file
	  */
	 public static void readerFileDataToIgnite(String filePath) {
		  Gson gson = new Gson();
		  BufferedReader reader = null;
		  try {
			   File file = new File(filePath);
			   if (!file.isFile()) {
			   log.info("file is null");
			   return;
		   }
			   //reader =  new  BufferedReader()
		   reader = new BufferedReader(new FileReader(file));
		   List<ImmutablePair<String,Object>> dataList = new ArrayList<ImmutablePair<String,Object>>();
		   String line = "";
		   while ((line = reader.readLine()) != null) {
			    @SuppressWarnings("unchecked")
			    ImmutablePair<String,Object> immutablePair = gson.fromJson(line, ImmutablePair.class);
			    dataList.add(immutablePair);
			    if(dataList.size()>1000) {
			    	// 遍历放入cache中
				   	dataList.forEach(immutablePair1 -> {				   		
				    //region.putAsync((String) immutablePair.getLeft(), immutablePair.getRight());
				   });
				   	dataList.clear();
			    }
		   }
		   	// 遍历放入cache中
		   	dataList.forEach(immutablePair -> {
		    //region.putAsync((String) immutablePair.getLeft(), immutablePair.getRight());
		   });
		   	dataList.clear();
		   	//Stream<String> ss= reader.lines()
		  } catch (Exception e) {
			  log.error("读取文件中的数据存入ignite出错", e);
		  } finally {
			   try {
			    if (reader != null) {
			     reader.close();
			    }
			   } catch (IOException e) {
				   log.error("流关闭出错！", e);
			   }
		   
		  }
	 }
		

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
