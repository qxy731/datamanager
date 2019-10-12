package com.myboot.dataprocess.process.phoenix;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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
     * @throws IOException 
     */
    public Connection getConnection() {
    	 try {
    		 Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
    		 String url = myKafkaConfiguration.getOtherParameter("jdbc.url");
    		 //log.info(url);
    		 //String url = "jdbc:phoenix:122.112.198.196,122.112.255.137,122.112.224.4";
    		 //String url = "jdbc:phoenix:ecs---2-0004,ecs---2-0005,ecs---2-0001";
    		 return DriverManager.getConnection(url);
         } catch (Exception e) {
             log.error("[hbase]获取连接异常!!" + e.getMessage());
             e.printStackTrace();
         }
    	 return null;
    }
    
	public List<Map<String,String>> query(String sql) throws Exception {
		List<Map<String,String>> list = new ArrayList<Map<String,String>>();
		Connection conn = getConnection();
		ResultSet rs = null;
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sql);
		    rs = stmt.executeQuery();
		    if(rs != null) {
		    	Map<String,String> map = null;
		    	ResultSetMetaData meta = rs.getMetaData();
		    	int length = meta.getColumnCount();
		    	while(rs.next()) {
		    		map = new LinkedHashMap<String,String>();
		    		for(int i=0; i<length;i++) {
		    			String column = meta.getColumnName(i);
		    			String value = rs.getString(i);
		    			map.put(column,value);
		    			list.add(map);
		    		}
		    	}
		    }
		}catch(Exception e) {
			log.error(e.getMessage());
		}finally {
			try {
				stmt.close();
				rs.close();
				conn.close();
			}catch(Exception e) {
				//e.printStackTrace();
			}
		}
		return list;
	}
	
	public void save(Map<String,String> map) throws Exception {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = getConnection();
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
				values.append(value);
				if(count<size) {
					columns.append(",");
					values.append(",");
				}
			}
			sb.append("(").append(columns).append(")");
			sb.append(" values (").append(values).append(")");
			//"upsert into tab(col1,col2) values(1,'test1')"
			log.info("insert into phoenix table sql:"+sb.toString());
			stmt = conn.prepareStatement(sb.toString());
			stmt.execute();
		}catch(Exception e) {
			log.error(e.getMessage());
		}finally {
			try {
				stmt.close();
				conn.close();
			}catch(Exception e) {
				//e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
