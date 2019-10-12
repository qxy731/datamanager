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
    		 return DriverManager.getConnection(url);
         } catch (Exception e) {
             log.error("[phoenix]获取连接异常!!" + e.getMessage());
             //e.printStackTrace();
         }
    	 return null;
    }
    
	public List<Map<String,String>> query(String sql) throws Exception {
		List<Map<String,String>> list = new ArrayList<Map<String,String>>();
		Connection conn = getConnection();
		ResultSet rs = null;
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sql.toLowerCase());
		    rs = stmt.executeQuery();
		    if(rs != null) {
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
		}catch(Exception e) {
			log.error(e.getMessage());
		}finally {
			try {
				stmt.close();
				rs.close();
				conn.close();
			}catch(Exception e) {
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
				values.append("'");
				values.append(value);
				values.append("'");
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
		MyPhoenixProcessRepository p = new MyPhoenixProcessRepository();
		try {
			List<Map<String,String>> query = p.query("select * from \"hbs_trans_log_act\" limit 2");
			System.out.println(query.size());
			System.out.println(query.get(0).toString());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
