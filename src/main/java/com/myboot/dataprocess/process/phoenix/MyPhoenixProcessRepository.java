package com.myboot.dataprocess.process.phoenix;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.myboot.dataprocess.process.kafka.common.MyKafkaConfiguration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class MyPhoenixProcessRepository {
	
	@Autowired
	private MyKafkaConfiguration myKafkaConfiguration;
	
    /**
     * 获取连接
     * @return
     * @throws ClassNotFoundException 
     * @throws SQLException 
     * @throws IOException 
     */
    public Connection getConnection() throws ClassNotFoundException, SQLException {
		 Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		 String url = myKafkaConfiguration.getOtherParameter("jdbc.url");
		 return DriverManager.getConnection(url);
    }
    
	public List<Map<String,String>> query(String sql) throws Exception {
		List<Map<String,String>> list = new ArrayList<Map<String,String>>();
		try(Connection conn = getConnection()) {
			try(PreparedStatement stmt = conn.prepareStatement(sql.toLowerCase())){
			    try(ResultSet rs = stmt.executeQuery()){
			    	ResultSetMetaData meta = rs.getMetaData();
			    	int length = meta.getColumnCount();
			    	while(rs.next()) {
			    		Map<String,String> map = new LinkedHashMap<String,String>();
			    		for(int i=1; i<length;i++) {
			    			String column = meta.getColumnLabel(i);
			    			String value = rs.getString(i);
			    			map.put(column,value);
			    		}
			    		list.add(map);
			    	}
			    }
			}
		}catch(Exception e) {
			log.error(e.getMessage());
		}finally {
		}
		return list;
	}
	
	public void save(Map<String,String> map) throws Exception {
		try(Connection conn = getConnection()) {
			String sql = map2Sql(map);
			try(PreparedStatement stmt = conn.prepareStatement(sql)){
				stmt.executeUpdate();
			}
			conn.commit();
		}catch(Exception e) {
			log.error(e.getMessage());
		}finally {
			
		}
	}
	
	
	public String map2Sql(Map<String,String> map) {
		StringBuffer sb = new StringBuffer("upsert into ");
		String tablename = myKafkaConfiguration.getOtherParameter("phoenix_tablename");
		sb.append(tablename);
		StringBuffer columns = new StringBuffer();
		StringBuffer values = new StringBuffer();
		int size = map.size();
		int count = 0;
		for(Map.Entry<String, String> entry : map.entrySet()) {
			count++;
			String key = entry.getKey();
			String value = entry.getValue();
			columns.append(key);
			values.append("'").append(value).append("'");
			if(count<size) {
				columns.append(",");
				values.append(",");
			}
		}
		sb.append("(").append(columns).append(")");
		sb.append(" values (").append(values).append(")");
		//"upsert into tab(col1,col2) values(1,'test1')"
		log.info("upsert into phoenix table sql:"+sb.toString());
		return sb.toString();
	}
	
	public void saveByMap(List<Map<String,String>> list){
		List<String> sqls = new LinkedList<String>();
		for(int i=0;i<list.size();i++) {
			Map<String,String> map = list.get(i);
			String sql = map2Sql(map);
			sqls.add(sql);
		}
		this.saveBySql(sqls);
		return;
	}
	
	public void saveBySql(List<String> sqls) {
		if(sqls == null || sqls.size() == 0) {
			return;
		}
		try(Connection conn = getConnection()) {
			conn.setAutoCommit(false);
			for(String sql : sqls) {
				try (PreparedStatement stmt = conn.prepareStatement(sql)) {
				    stmt.executeUpdate();
				}
			}
			conn.commit();
		}catch(Exception e) {
			log.info("saveBySql throw excepton ...");
			log.error(e.getMessage());
		}finally {
			sqls.clear();
		}
	}
	
	
	public static void main(String[] args) {
		
	}

}
