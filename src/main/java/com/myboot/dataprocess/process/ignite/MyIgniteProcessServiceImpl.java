package com.myboot.dataprocess.process.ignite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.ignite.IgniteCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.myboot.dataprocess.process.phoenix.MyPhoenixProcessRepository;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class MyIgniteProcessServiceImpl implements MyIgniteProcessService {

	@Autowired
	private MyPhoenixProcessRepository repository;
	
	private List<ImmutablePair<String, Object>> dataList = new ArrayList<ImmutablePair<String, Object>>();
	
	/**
	 * key = 日期
	 * value = count值
	 * 同文件同纬度同条件下的数据
	 */
	Map<String, Long> data = new HashMap<String, Long>();
	
 	IgniteFilePojo tmpPojo = new IgniteFilePojo();
 	
	@Override
	public Map<String,Object> query(Map<String,String> params) throws Exception {
		//SqlFieldsQuery query = new SqlFieldsQuery(sql);
		//query.setDistributedJoins(true);
		IgniteCache<String, Object> cache = MyIgniteRepository.getIgniteCache();
		String fileNo = params.get("fileNo")==null?"":params.get("fileNo").toString();
		String key1Value = params.get("key1")==null?"":params.get("key1").toString();
		String key2Value = params.get("key2")==null?"":params.get("key2").toString();
		IgniteFilePojo  pojo = IgniteCommonUtil.getDerivedVariableKey(fileNo);
		String[] arr = pojo.getArr();
		int d = pojo.getDimension();
		//String key2 = pojo.getKey2();
		Map<String,Object> map = new HashMap<String,Object>();
		for (int i = 0; i < arr.length; i++) {
			String key = arr[i];
			if(d==0) {
				//key = key + "1";
			} else if(d==1) {
				key = key + key1Value;
			}else if(d==2) {
				key = key.replace("#key1#", key1Value)+key2Value;
			}
			VersionCache vc = (VersionCache)cache.get(key);
			map.put(key, vc);
		}
		return map;
	}

	@Override
	public Map<String, Object> save(String[] fileNames) throws Exception {
		Map<String,Object> retMap =  new LinkedHashMap<String,Object>();
		Map<String,String> sqls = IgniteCommonUtil.getDataQuerySQL(fileNames);
		int cnt = 0;
		for(Map.Entry<String,String> sqlObject : sqls.entrySet()) {
			String fileNo = sqlObject.getKey();
			String sql = sqlObject.getValue();
			int total = saveByPhoenixData(fileNo,sql);
			retMap.put(fileNames[cnt], total);
			cnt++;
		}
		return retMap;
	}
	
	private int saveByPhoenixData(String fileNo,String sql) {
		int total = 0;
		int igniteCount = 0;
		try(Connection conn = repository.getConnection()) {
			try (PreparedStatement stmt = conn.prepareStatement(sql)) {
				//写入相应的文件
		        ResultSet rs = stmt.executeQuery(sql);
		        long start = System.currentTimeMillis();
		        while (rs.next()) {
		        	total++;
		        	ResultSetMetaData meta = rs.getMetaData();
		        	int length = meta.getColumnCount();
		    		Map<String,Object> map = new LinkedHashMap<String,Object>();
		    		for(int i = 1; i<= length;i++) {
		    			String column = meta.getColumnLabel(i);
		    			String value = rs.getString(i);
		    			map.put(column,value);
		    		}
		    		//拼装ignite对象方法
	    			setDerivedVariableData(fileNo,map);
			        //清除缓存
			    	if(total % 50000==0) {
			    		MyIgniteRepository.save2Ignite(dataList);
			    		dataList.clear();
			    		igniteCount += 50000;
			    	}
		          }
		        MyIgniteRepository.save2Ignite(dataList);
		        dataList.clear();
		        igniteCount += dataList.size();
		        long end = System.currentTimeMillis();
		        log.info("MyIgniteProcessServiceImpl#saveByPhoenixData：fileNo( "+ fileNo + ") phoenix data number:"+total+". cost:"+(end-start)+"ms");
		        log.info("MyIgniteProcessServiceImpl#saveByPhoenixData：fileNo( "+ fileNo + ") ignite data number:"+igniteCount+".");
			}
		}catch(Exception e) {
			log.info("MyIgniteProcessServiceImpl#saveByPhoenixData throw excepton ...");
			log.error(e.getMessage());
		}finally {
			
		}
		return total;
	}
	
	
	private void resetTmpPojoDiffFileNo(String fileNo,Map<String,Object> map) {
		clearData();
		tmpPojo = IgniteCommonUtil.getDerivedVariableKey(fileNo);
		int dimension = tmpPojo.getDimension();//纬度数量0、1、2
		String key1 = tmpPojo.getKey1();// 主维度
	 	String key2 = tmpPojo.getKey2();// 第二维度
		if(dimension==1) {
			long newVal1 = Long.valueOf(map.get(key1).toString());
			tmpPojo.setKey1Value(newVal1);
		}else if(dimension==2) {
			long newVal1 = Long.valueOf(map.get(key1).toString());
			long newVal2 = Long.valueOf(map.get(key2).toString());
			tmpPojo.setKey1Value(newVal1);
			tmpPojo.setKey2Value(newVal2);
		}
	}
	
	private void resetTmpPojoBySameFile(Map<String,Object> map) {
		int dimension = tmpPojo.getDimension();//纬度数量0、1、2
		String key1 = tmpPojo.getKey1();// 主维度
	 	String key2 = tmpPojo.getKey2();// 第二维度
	 	long oldVal1 = tmpPojo.getKey1Value();
	 	long oldVal2 = tmpPojo.getKey2Value();
		if(dimension==1) {
			long newVal1 = Long.valueOf(map.get(key1).toString());
			if(oldVal1 != newVal1) {
				clearData();
			}
			tmpPojo.setKey1Value(newVal1);
		}else if(dimension==2) {
			long newVal1 = Long.valueOf(map.get(key1).toString());
			long newVal2 = Long.valueOf(map.get(key2).toString());
			if(oldVal1 != newVal1 || oldVal2 != newVal2 ) {
				clearData();
			}
			tmpPojo.setKey1Value(newVal1);
			tmpPojo.setKey2Value(newVal2);
		}
	}
	
	
	/**
	 * 
	 * @param fileNo
	 * @param map
	 * @return
	 */
	private void setDerivedVariableData(String fileNo,Map<String,Object> map) {
		 //根据文件编号获取维度信息
		String tmpFileNo = tmpPojo.getFileNo();
		if(!tmpFileNo.equals(fileNo)) {
			resetTmpPojoDiffFileNo(fileNo,map);
		}else {
			resetTmpPojoBySameFile(map);
		}
		// 文件编号不存在
		if (tmpPojo == null) {
			return;
		}
		putData(map);
	}
	
	private void clearData() {
		String[] arr = tmpPojo.getArr();// ignite中存储的key
		int dimension = tmpPojo.getDimension();//纬度数量0、1、2
	 	long oldVal1 = tmpPojo.getKey1Value();
	 	long oldVal2 = tmpPojo.getKey2Value();
		for (int i = 0; i < arr.length; i++) {
			VersionCache vc = VersionCache.of(data, data.size());
			String key = arr[i];
			if(dimension == 1) {
				key = key + oldVal1;
			}else if(dimension == 2) {
				key = key.replaceAll("#key1#", oldVal1+"") + oldVal2;
			}
			ImmutablePair<String, Object> rowData = new ImmutablePair<String, Object>(key, vc);
			dataList.add(rowData);
		}
		data.clear();
	}
	
	private void putData(Map<String,Object> map) {
		String[] arr = tmpPojo.getArr();// ignite中存储的key
		for (int i = 0; i < arr.length; i++) {
			// 获取日期
			String applicationdate = map.get("APPLICATIONDATE")==null?"20191013":map.get("APPLICATIONDATE").toString();
			applicationdate = applicationdate.replaceAll("-", "").substring(0, 8);
			// 获取统计数
			long count = map.get("COUNT")==null?0:Long.valueOf(map.get("COUNT").toString());
			data.put(applicationdate, count);
		}
	}
	

}
