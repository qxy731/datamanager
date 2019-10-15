package com.myboot.dataprocess.process.kafka.producer.history;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.myboot.dataprocess.process.hbase.MyHbaseProcessRepository;
import com.myboot.dataprocess.process.hbase.common.MyHbaseConfiguration;
import com.myboot.dataprocess.process.kafka.common.MyKafkaConfiguration;
import com.myboot.dataprocess.process.kafka.producer.MyKafkaProducerHistoryService;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class MyKafkaProducerHbaseServiceImpl implements MyKafkaProducerHistoryService{
	
	@Autowired
    private MyHbaseConfiguration myHbaseConfiguration;
	
	@Autowired
	private MyKafkaConfiguration myKafkaConfiguration;
	
	@Autowired
	private MyHbaseProcessRepository hbaseRepository;
	
    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

	@Override
	public void sendHisMessage(String topic, Map<String,Object> params) {
		String tableName = myHbaseConfiguration.getOtherParameter("tablename");
		String batchstr = myHbaseConfiguration.getOtherParameter("scan.batch");
		int batch = Integer.valueOf(batchstr);
		try {
			hbaseRepository.getConnection();
			//列簇名称
			String columnFamily = myHbaseConfiguration.getOtherParameter("columnfamily");
			int count = 0;
			String startRowKey = null;
			//第一次获取数据
			LinkedHashMap<String,LinkedHashMap<String,Object>> scanByPageSizeAll = hbaseRepository.scanByPageSizeAll(tableName, columnFamily,"", batch);
			if(topic==null || topic.length()<1) {
        		topic  = myKafkaConfiguration.getOtherParameter("source.topic");
        	}
			while(scanByPageSizeAll.size()>0){
				for(Map.Entry<String, LinkedHashMap<String, Object>> entry:scanByPageSizeAll.entrySet()){
						System.out.println("rowkey" + entry.getKey());
	                    LinkedHashMap<String, Object> map = entry.getValue();
	                    map.putAll(params);
	                    Gson gson = new Gson();
	                	String jsonStr = gson.toJson(map);
	                    kafkaTemplate.send(topic,jsonStr);
					    count++;
					    log.info("send the "+count+"number hbase history data message :" + jsonStr);
				}
				//继续获取数据
				if(scanByPageSizeAll.size() > 0){
					startRowKey = getTailByReflection(scanByPageSizeAll).getKey() + 0;
				    scanByPageSizeAll = hbaseRepository.scanByPageSizeAll(tableName, columnFamily,startRowKey, batch);
				}
			}
		} catch (Exception e) {
			//e.printStackTrace();
			log.error(e.getMessage());
		}finally {
			if(null != hbaseRepository) {
				try {
					hbaseRepository.closeConnection();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	public <K, V> Entry<K, V> getTailByReflection(LinkedHashMap<K, V> map)
	        throws NoSuchFieldException, IllegalAccessException {
	    Field tail = map.getClass().getDeclaredField("tail");
	    tail.setAccessible(true);
	    @SuppressWarnings("unchecked")
		Entry<K, V> entry = (Entry<K, V>) tail.get(map);
		return entry;
	}

}
