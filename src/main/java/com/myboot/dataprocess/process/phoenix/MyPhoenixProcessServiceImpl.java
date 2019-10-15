package com.myboot.dataprocess.process.phoenix;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.myboot.dataprocess.process.kafka.common.MyKafkaConfiguration;

import lombok.extern.slf4j.Slf4j;


@Slf4j
@Service
public class MyPhoenixProcessServiceImpl implements MyPhoenixProcessService{
	
	private static final int COMMIT_SIZE = 1000;
	
	private ExecutorService E1 = Executors.newSingleThreadExecutor();
	
	private ScheduledExecutorService E2 = Executors.newScheduledThreadPool(1);
	
	List<String> sqls = new LinkedList<String>();
	
	int cnt = 0;
	
	{
	    E2.scheduleWithFixedDelay(() -> {
	    	processPhoenix(null,1);
	    }, 5L, 3L, TimeUnit.SECONDS);
	}
	
	@Autowired
	private MyPhoenixProcessRepository repository;
	
	@Autowired
	private MyKafkaConfiguration myKafkaConfiguration;
	
	@Override
	public List<Map<String, String>> query(String sql) throws Exception {
		return repository.query(sql);
	}

	@Override
	public void save(Map<String, String> map) throws Exception {
		repository.save(map);
	}
	
	@Override
	public void save(List<String> sqls) throws Exception {
		repository.saveBySql(sqls);
	}
	
    /**
     * 
     * 向phoenix保存数据
     * @throws Exception 
     */
	@Override
	public void processPhoenix(Map<String, Object> mapMessage,int type) {
		E1.execute(() -> {
			try {
				if (type == 0) {
			        if (++cnt < COMMIT_SIZE) {
			        	processPhoenixImpl(mapMessage);
			        	return;
			        }
			      }
			      if (sqls.size() == 0) {
			    	  return;
			      }
			      try(Connection conn = repository.getConnection()) {
						conn.setAutoCommit(false);
						for(String sql : sqls) {
							try (PreparedStatement stmt = conn.prepareStatement(sql)) {
							    stmt.executeUpdate();
							}catch(Exception e) {
								
							}
						}
						conn.commit();
			      } catch (Exception e) {
			    	  
			      } finally {
			        cnt = 0;
			        sqls.clear();
			      }
			}catch(Exception e) {
	    		log.error("send source message to phoenix is fail...");
	    		log.error(e.getMessage());
	    	}
    	});
	}
	
	private void processPhoenixImpl(Map<String, Object> mapMessage) {
		StringBuffer sb = new StringBuffer("upsert into ");
		String tablename = myKafkaConfiguration.getOtherParameter("phoenix_tablename");
		sb.append(tablename);
		StringBuffer columns = new StringBuffer();
		StringBuffer values = new StringBuffer();
		int size = mapMessage.size();
		int count = 0;
		for(Map.Entry<String,Object> entry : mapMessage.entrySet()) {
			count++;
			String key = entry.getKey();
		    Object value = entry.getValue();
		    String cvalue = "";
		    if(value != null) {
			     cvalue = String.valueOf(value);
			}
		    columns.append(key.toUpperCase());
			values.append("'").append(cvalue).append("'");
			if(count<size) {
				columns.append(",");
				values.append(",");
			}
			sb.append("(").append(columns).append(")");
			sb.append(" values (").append(values).append(")");
			log.info("upsert into phoenix table sql:"+sb.toString());
			if(sqls.size()<COMMIT_SIZE) {
				sqls.add(sb.toString());
			}
		}
	}
	
}