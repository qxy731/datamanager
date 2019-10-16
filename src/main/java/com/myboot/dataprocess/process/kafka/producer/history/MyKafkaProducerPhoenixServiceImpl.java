package com.myboot.dataprocess.process.kafka.producer.history;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.myboot.dataprocess.model.KafkaApplyCardEntity;
import com.myboot.dataprocess.process.kafka.common.KafkaDataModelProcess;
import com.myboot.dataprocess.process.kafka.common.MyKafkaConfiguration;
import com.myboot.dataprocess.process.kafka.producer.MyKafkaProducerHistoryService;
import com.myboot.dataprocess.process.phoenix.MyPhoenixProcessRepository;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class MyKafkaProducerPhoenixServiceImpl implements MyKafkaProducerHistoryService{
	
	@Autowired
	private MyPhoenixProcessRepository repository;
	
	@Autowired
	private MyKafkaConfiguration myKafkaConfiguration;
	
    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;
    
    @Autowired
    private KafkaDataModelProcess kafkaDataModelProcess;
    
	private ScheduledExecutorService E2 = Executors.newScheduledThreadPool(1);
	
	//private long mstart = System.currentTimeMillis() ;
	
	private static int total = 0;
	
	{
	    E2.scheduleWithFixedDelay(() -> {
	    	log.info("从phoenix往kafka已经加载"+total+"条数据。");
	    }, 5L, 3L, TimeUnit.SECONDS);
	}
	
    
    /**
     * 从phoenix源表中读取1000W数据推送到kafka
     * @throws Exception 
     */
	@Override
	public void sendHisMessage(String topic, Map<String,Object> params) {
		try(Connection conn = repository.getConnection()) {
			String tablename = myKafkaConfiguration.getOtherParameter("phoenix_tablename");
			if(topic==null || topic.length()<1) {
				topic  = myKafkaConfiguration.getOtherParameter("source.topic");
			}
			String sql = "select * from "+ tablename;
			try (PreparedStatement stmt = conn.prepareStatement(sql)) {
		        ResultSet rs = stmt.executeQuery(sql);
		        long start = System.currentTimeMillis();
		        while (rs.next()) {
		        	ResultSetMetaData meta = rs.getMetaData();
		        	int length = meta.getColumnCount();
		    		Map<String,Object> map = new LinkedHashMap<String,Object>();
		    		for(int i=1; i<length;i++) {
		    			String column = meta.getColumnLabel(i);
		    			String value = rs.getString(i);
		    			map.put(column,value);
		    		}
			        map.putAll(params);
			        KafkaApplyCardEntity entity = kafkaDataModelProcess.processHisKafkaData(map);
			        Gson gson = new Gson();
			    	String jsonStr = gson.toJson(entity);
			    	log.info("send phoenix his data message :" + jsonStr);
			        kafkaTemplate.send(topic,jsonStr);
		            total++;
		          }
		          log.info("================"+(System.currentTimeMillis() - start) + ": " + total + "================");
			}
		}catch(Exception e) {
			log.info("sendHisMessage throw excepton ...");
			log.error(e.getMessage());
		}finally {
			
		}
		
	}
	

}
