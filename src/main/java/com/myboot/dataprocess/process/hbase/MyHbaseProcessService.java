package com.myboot.dataprocess.process.hbase;

import java.io.IOException;
import java.util.Map;

/** 
*
* @ClassName ：HbaseProcessService 
* @Description ： 业务逻辑接口类
* @author ：PeterQi
*
*/
public interface MyHbaseProcessService {
	
	/**
	 * 创建表
	 * @throws Exception
	 */
	public void create() throws IOException;
	/**
	 * 创建表
	 * @param namespace
	 * @param tableName
	 * @param columnFamily
	 * @throws Exception
	 */
	public void create(String namespace,String tableName,String columnFamily) throws Exception;
	/**
	 * 删除表
	 * @param namespace
	 * @param tableName
	 * @throws Exception
	 */
	public void drop(String tableName) throws Exception;
	/**
	 * 插入或更新数据
	 * @param total
	 * @param beginDate
	 * @param endDate
	 * @throws Exception
	 */
	public void save(int total,String beginDate,String endDate) throws Exception;
	
	/**
	 * 删除表数据
	 * @param tableName
	 * @param rowkey
	 * @throws Exception
	 */
	public void delete(String tableName,String rowkey) throws Exception;
	
	/**
	 * 根据params查询数据
	 * @param tableName
	 * @param params
	 * @throws Exception
	 */
	public void select(String tableName,Map<String,Object> params) throws Exception;
	
	public void process() throws Exception;
	
	public void other() throws Exception;
	

}
