package com.myboot.dataprocess.process.hbase.common;

import java.util.LinkedHashMap;
import java.util.Map;

import com.myboot.dataprocess.builder.RandomDataModelBuilder;
import com.myboot.dataprocess.builder.RowkeyGenerator;
import com.myboot.dataprocess.model.ApplyCardEntity;

public class HbaseDataModelProcess {
	
	public static Map<String,Object> assembleHbaseData(Integer count,String currentDate) {
		Map<String,Object> map = new LinkedHashMap<String,Object>();
		for(int i=0;i<count;i++) {
			RowkeyGenerator generator = RowkeyGenerator.getInstance(currentDate);
			String rowkey = generator.getRowkey();
			long sequence = generator.getSequence();
			ApplyCardEntity data = RandomDataModelBuilder.getRandomDataModel(sequence, currentDate);
		    map.put(rowkey, data);
		    //System.out.println("{"+rowkey+"="+data+"}");
		}
		return map;
	}
	
	public static void main(String[] args) {
		int count = 10000;
		String currentDate = "2019-10-01";
		HbaseDataModelProcess.assembleHbaseData(count,currentDate);
	}

}
