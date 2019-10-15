package com.myboot.dataprocess.process.hbase;

import java.io.IOException;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.myboot.dataprocess.process.hbase.common.HbaseDataModelProcess;
import com.myboot.dataprocess.process.hbase.common.MyHbaseConfiguration;
import com.myboot.dataprocess.tools.CommonTool;

import lombok.extern.slf4j.Slf4j;

/** 
*
* @ClassName ：HbaseProcessService 
* @Description ： 业务逻辑实现类
* @author ：PeterQi
*
*/
@Slf4j
@Service
public class MyHbaseProcessServiceImpl implements MyHbaseProcessService {
	
	@Autowired
	private MyHbaseProcessRepository hbaseRepository;
	
	@Autowired
    private MyHbaseConfiguration myHbaseConfiguration;
	
/*	@Autowired
	private MyKafkaConfiguration myKafkaConfiguration;*/
	
	/**
	 * 创建表
	 */
	@Override
	public void create() throws IOException {
		log.info("===================================================== method is start ===================================================== ");
		try {
			//空间名称
			String namespace = myHbaseConfiguration.getOtherParameter("namespace");
			//数据表表名
			String tableName =  myHbaseConfiguration.getOtherParameter("tablename");
			//列簇名称
			String columnFamily = myHbaseConfiguration.getOtherParameter("columnFamily");
			hbaseRepository.getConnection();
			hbaseRepository.create(namespace, tableName,columnFamily);
		} catch (Exception e) {
			e.printStackTrace();
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
		log.info("===================================================== method is end ===================================================== ");
	
	}
	
	/**
	 * 创建表
	 */
	@Override
	public void create(String namespace, String tableName, String columnFamily) throws Exception {
		log.info("===================================================== method is start ===================================================== ");
		try {
			hbaseRepository.getConnection();
			hbaseRepository.create(namespace, tableName,columnFamily);
		} catch (Exception e) {
			e.printStackTrace();
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
		log.info("===================================================== method is end ===================================================== ");
	
	}
	
	/**
	 * 删除表
	 */
	@Override
	public void drop(String tableName) throws Exception {
		log.info("===================================================== method is start ===================================================== ");
		try {
			hbaseRepository.getConnection();
			hbaseRepository.drop(tableName);
		} catch (Exception e) {
			e.printStackTrace();
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
		log.info("===================================================== method is end ===================================================== ");
	
	}
	
	/**
	 * 插入或更新表数据
	 */
	@Override
	public void save(int total, String startDate, String endDate) throws Exception {
		log.info("===================================================== method is start ===================================================== ");
		try {
			String tableName =  myHbaseConfiguration.getOtherParameter("tablename");
			String columnFamily = myHbaseConfiguration.getOtherParameter("columnfamily");
			int days = CommonTool.betweenDay(startDate, endDate);
			int dayTotal = total/days;
			int modTotal = total%days;
			String currentDate = startDate;
			hbaseRepository.getConnection();
			for(int d = 0; d < days; d++) {
				if(d == days - 1 && modTotal != 0 ) {
					dayTotal = dayTotal + modTotal;
				}
				int count = 100000;
				int length = dayTotal/count;
				int mod = dayTotal % count;
				if(mod != 0 ) {
					length = length+1;
				}
				for(int i=0;i<length;i++) {
					if(i == length-1 && mod != 0) {
						count = mod;
					}
					log.info("========================insert start==================================");
					log.info("betweenDay:"+days+"&dayTotal:"+dayTotal+"&modTotal:"+modTotal+"&currentDate:"+currentDate+"&count:"+count+"&length:"+length+"&mod:"+mod);
					log.info("========================insert end====================================");
					Map<String,Object> map = HbaseDataModelProcess.assembleHbaseData(count,currentDate);
					hbaseRepository.insert(tableName,columnFamily,map);
				}
				currentDate = CommonTool.addDay(currentDate,1);
			}
		} catch (Exception e) {
			e.printStackTrace();
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
		log.info("===================================================== method is end ===================================================== ");
	}
	
	@Override
	public void delete(String tableName, String rowkey) throws Exception {
		
	}


	@Override
	public void select(String tableName, Map<String, Object> params) throws Exception {
		log.info("===================================================== method is start ===================================================== ");
		try {
			hbaseRepository.getConnection();
			String rowkey = params.get("rowkey")==null?"":params.get("rowkey").toString();
			hbaseRepository.select(tableName, rowkey);
		} catch (Exception e) {
			e.printStackTrace();
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
		log.info("===================================================== method is end ===================================================== ");
	}
	
	public void select(String tableName, String rowkey) throws Exception {
		log.info("===================================================== method is start ===================================================== ");
		try {
			hbaseRepository.getConnection();
			hbaseRepository.select(tableName, rowkey);
		} catch (Exception e) {
			e.printStackTrace();
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
		log.info("===================================================== method is end ===================================================== ");
	}
	

	public static void main(String[] args) {
		
	}
	
}