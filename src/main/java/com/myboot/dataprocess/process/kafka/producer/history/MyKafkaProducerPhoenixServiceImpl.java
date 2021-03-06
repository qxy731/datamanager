package com.myboot.dataprocess.process.kafka.producer.history;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;

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
    
	//private ScheduledExecutorService E2 = Executors.newScheduledThreadPool(1);
	
	private static int total = 0;
	
	/*{
	    E2.scheduleWithFixedDelay(() -> {
	    	log.info("从phoenix往kafka已经加载"+total+"条数据。");
	    }, 5L, 3L, TimeUnit.SECONDS);
	}
	*/
    
    /**
     * 从phoenix源表中读取1000W数据推送到kafka
     * @throws Exception 
     */
	@Override
	public void sendHisMessage(String topic, Map<String,Object> params) {
		total = 0;
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
		        	total++;
		        	ResultSetMetaData meta = rs.getMetaData();
		        	int length = meta.getColumnCount();
		    		Map<String,Object> map = new HashMap<String,Object>();
		    		for(int i=1; i<=length;i++) {
		    			String column = meta.getColumnLabel(i);
		    			String value = rs.getString(i);
		    			map.put(column,value);
		    		}
			        map.putAll(params);
			        KafkaApplyCardEntity entity = kafkaDataModelProcess.processHisKafkaData(map);
			        Gson gson = new Gson();
			    	String jsonStr = gson.toJson(entity);
			    	//log.info("MyKafkaProducerPhoenixServiceImpl#sendHisMessage: send phoenix his data message to source topic kafka:" + jsonStr);
			        kafkaTemplate.send(topic,jsonStr);
		          }
		          log.info("MyKafkaProducerPhoenixServiceImpl#sendHisMessage "+total+"total cost:"+(System.currentTimeMillis() - start) + "ms" );
			}
		}catch(Exception e) {
			log.info("MyKafkaProducerPhoenixServiceImpl#sendHisMessage throw excepton ...");
			log.error(e.getMessage());
		}finally {
			
		}
		
	}
	

}
