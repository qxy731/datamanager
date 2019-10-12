package com.myboot.dataprocess.process.hbase;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.myboot.dataprocess.process.hbase.common.MyHbaseConfiguration;

import lombok.extern.slf4j.Slf4j;
/** 
*
* @ClassName ：MyHbaseProcessTool 
* @Description ： Hbase DDL&DML处理实体类
* @author ：PeterQi
*
*/
@Component
@Slf4j
public class MyHbaseProcessRepository {
	
    @Autowired
    private MyHbaseConfiguration myHbaseConfiguration;
	
    private Connection connection = null;
    
    /**
     * 获取连接
     * @return
     * @throws IOException 
     */
    public Connection getConnection() throws IOException {
        if (connection == null || connection.isClosed()) {
        	connection = ConnectionFactory.createConnection(myHbaseConfiguration.configuration());
        }
        return connection;
    }
    
    /**
	 * 关闭连接
	 * @throws IOException
	 */
	public void closeConnection() throws IOException{
		if(null != connection && !connection.isClosed()){
			connection.close();
		}
	}
	
	/**
	 * 创建表
	 * @throws IOException
	 */
	public void create(String namespace,String tableName,String columnFamily) throws IOException{
		log.info("---------------创建表 START-----------------");
		Admin admin = connection.getAdmin();
		//admin.createNamespace(NamespaceDescriptor.create(namespace).build());
		//新建一个数据表表名对象
		TableName tn = TableName.valueOf(tableName);
		//判断表是否存在
		if(admin.tableExists(tn)){
			log.info(tableName+"表已经存在！");
		}else{
			log.info(tableName+"表不存在，开始创建！");
			//数据表描述对象
			HTableDescriptor tbDescriptor=new HTableDescriptor(tn);
			//列族描述对象
			if(null != columnFamily && columnFamily.length()>0) {
				HColumnDescriptor f1 = new HColumnDescriptor(columnFamily);
				//在数据表中新建一个列族
				tbDescriptor.addFamily(f1);
			}
			//新建数据表
			//admin.createTable(tbDescriptor);
			String[] splitArray = "10|20|30|40|50|60|70|80|90".split("|");
			byte[][] splitKeys = new byte[splitArray.length][];
			for(int i=0;i<splitArray.length;i++) {
				splitKeys[i] = Bytes.toBytes(splitArray[i]);
			}
			admin.createTable(tbDescriptor,splitKeys);
			admin.close();
			log.info(tableName+"表创建成功！");
		}
		log.info("---------------创建表 END-----------------");
	}
    
	/**
	 * 删除表
	 * @throws IOException
	 */
	public void drop(String tableName) throws IOException{
		log.info("---------------删除表 START-----------------");
		Admin admin = connection.getAdmin();
		// 新建一个数据表表名对象
		TableName tn = TableName.valueOf(tableName);
		//判断表是否存在
		if(admin.tableExists(tn)){
			admin.disableTable(tn);
			admin.deleteTable(tn);
			log.info(tableName+"表已经删除！");
		}else{
			log.info(tableName+"表不存在！");
		}
		admin.close();
		log.info("---------------删除表 END-----------------");
	}
	
	/**
	 * 删除数据
	 * @throws IOException
	 */
	public void delete(String tableName,String rowkey,String columnFamily,String cloumn) throws IOException{
		log.info("---------------删除表数据 START-----------------");
		// 新建一个数据表表名对象
		Table table = connection.getTable(TableName.valueOf(tableName));
		//指定rowkey
		Delete delete = new Delete(Bytes.toBytes(rowkey));
		//指定列
		if(columnFamily != null && cloumn != null) {
			delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(cloumn));
		}
		//执行删除操作
		table.delete(delete);
		table.close();
		log.info("---------------删除表数据 END-----------------");
	}
	/**
	 * 如果无列簇下面方面怎么更方便编写
	 */
	/**
	 * 单条单字段插入或更新数据
	 * @throws IOException
	 */
	public void insert(String tableName,String rowkey,String columnFamily,String cloumn,String columnText) throws IOException{
		log.info("---------------插入表 START-----------------");
		Table table = connection.getTable(TableName.valueOf(tableName));
		//单条插入
		Put put = new Put(Bytes.toBytes(rowkey));
		put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(cloumn), Bytes.toBytes(columnText));
		table.put(put);
		table.close();
		log.info(tableName + " 插入成功！");
		log.info("---------------插入表 END-----------------");
	}
	
	/**
	 * 单条插入或更新数据
	 * @throws IOException
	 */
	public void insert(String tableName,String rowkey,String columnFamily,String[] cloumn,String[] columnText) throws IOException{
		log.info("---------------插入表 START-----------------");
		Table table = connection.getTable(TableName.valueOf(tableName));
		//单条插入
		Put put = new Put(Bytes.toBytes(rowkey));
		for(int i = 0; i < cloumn.length; i++) {
			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(cloumn[i]), Bytes.toBytes(columnText[i]));
		}
		table.put(put);
		table.close();
		log.info(tableName + " 插入成功！");
		log.info("---------------插入表 END-----------------");
	}
	
	/**
	 * 单条插入或更新数据
	 * @throws IOException
	 */
	public void insert(String tableName,String rowkey,String[] columnFamily,String[] cloumn,String[] columnText) throws IOException{
		log.info("---------------插入表 START-----------------");
		Table table = connection.getTable(TableName.valueOf(tableName));
		//单条插入
		Put put = new Put(Bytes.toBytes(rowkey));
		for(int i = 0; i < cloumn.length; i++) {
			put.addColumn(Bytes.toBytes(columnFamily[i]), Bytes.toBytes(cloumn[i]), Bytes.toBytes(columnText[i]));
			//put.addC
		}
		table.put(put);
		table.close();
		log.info(tableName + " 插入成功！");
		log.info("---------------插入表 END-----------------");
	}
	
	/**
	 * 批量插入或更新数据
	 * @throws IOException
	 */
/*	public void insert(String tableName,String[] rowkey,String[] columnFamily,String[] cloumn,String[] columnText) throws IOException{
		log.info("---------------插入表 START-----------------");
		Table table = connection.getTable(TableName.valueOf(tableName));
		//单条插入
		List<Put> list = new ArrayList<Put>();
		for(int i = 0; i < cloumn.length; i++) {
			Put put = new Put(Bytes.toBytes(rowkey[i]));
			put.addColumn(Bytes.toBytes(columnFamily[i]), Bytes.toBytes(cloumn[i]), Bytes.toBytes(columnText[i]));
			list.add(put);
		}
		table.put(list);
		table.close();
		log.info(tableName + " 插入成功！");
		log.info("---------------插入表 END-----------------");
	}*/
	
	/**
	 * 批量插入或更新数据
	 * @throws IOException
	 * @throws IllegalAccessException 
	 * @throws IllegalArgumentException 
	 */
	public void insert(String tableName,String columnFamily,Map<String,Object> map) throws IOException, IllegalArgumentException, IllegalAccessException{
		log.info("---------------插入表 START-----------------");
		Table table = connection.getTable(TableName.valueOf(tableName));
		//单条插入
		List<Put> list = new ArrayList<Put>();
		int rowcount  = 0;
		for (Map.Entry<String,Object> entry : map.entrySet()) {
			String rowkey = entry.getKey();
			Object object = entry.getValue();
			log.info("{rowcount="+rowcount+",rowkey="+rowkey+",{columnFamily="+columnFamily+"&Object={"+object+"}}");
			Put put = new Put(Bytes.toBytes(rowkey));
			//int fieldcount = 0;
			//获取实体类的所有属性，返回Field数组
			Field[] fields = object.getClass().getDeclaredFields(); 
			  for(int i = 0;i < fields.length; i ++){
				   Field f = fields[i];
				   f.setAccessible(true);
				   //得到对应字段的属性名，
				   String column = f.getName();
				   column = column.substring(0, 1).toUpperCase()+column.substring(1);
				   //得到对应字段属性值
				   String columnText = (String)f.get(object);
				   //得到对应字段的类型
				   //Type type = f.getGenericType();
				   //log.info("{rowcount="+rowcount+",fieldcount="+fieldcount+",rowkey="+rowkey+",{columnFamily="+columnFamily+"&column="+column+"&columnText="+columnText+"}}");
				   if(StringUtils.isBlank(column)) {
					   //put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(columnText));
					  log.info("column:"+column);
				   }
				   if(StringUtils.isBlank(columnText)) {
					   //put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(columnText));
					  log.info("columnText:"+column);
				   }
				   
				  if(StringUtils.isNotBlank(columnText)&&StringUtils.isNotBlank(column)) {
					   //put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(columnText));
					  //log.info("column:"+column);
				   }
				  byte[] bytes = null;
				  if(StringUtils.isNotBlank(columnText)) {
					  bytes =  Bytes.toBytes(columnText);
				  }
				   put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), bytes);
				   if(i == fields.length-1) {
					   list.add(put);
				   }
				   //fieldcount++;
			  }
			  rowcount++;
		}
		table.put(list);
		table.close();
		log.info(tableName + " 插入成功！");
		log.info("---------------插入表 END-----------------");
	}
	
	/**
	 * 查询数据
	 * @throws IOException
	 */
	public void select(String tableName,String rowkey) throws IOException{
		log.info("---------------查询 START-----------------");
		//根据rowkey查询
		Table table =connection.getTable(TableName.valueOf(tableName));
		Get get = new Get(Bytes.toBytes(rowkey));
		//通过列族查询
//		get.addFamily(Bytes.toBytes("f1"));
		//通过列查询
//		get.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"));
		Result result = table.get(get);
		if(null != result){
			for (Cell cell:result.rawCells()) {
				log.info("列族:" + new String(CellUtil.cloneFamily(cell)) + " ");
				log.info("列:" + new String(CellUtil.cloneQualifier(cell)) + " ");
				log.info("值:" + new String(CellUtil.cloneValue(cell)) + " ");
				log.info("时间戳:" + cell.getTimestamp());
				log.info("---------------字段分隔符--------------");
			}
		}else{
			log.info("数据为空!!!!");
		}
		log.info("---------------查询 END-----------------");
	}
	
	/**
	 * 遍历查询数据
	 * @throws IOException
	 */
	public List<Map<String,Object>> selectAll(String tableName,String columnFamily) throws IOException{
		log.info("---------------查询 START-----------------");
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
		//根据rowkey查询
		Table table =connection.getTable(TableName.valueOf(tableName));
		//创建一个空的Scan实例
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(columnFamily));
        //在行上获取遍历器
        ResultScanner scanner = table.getScanner(scan);
		for (Result rs : scanner) {
			Map<String,Object> map = new LinkedHashMap<String,Object>();
            for (Cell cell : rs.listCells()) {
            	map.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
            }
            list.add(map);
        }
		log.info("---------------查询 END-----------------");
		return list;
	}
	
	/**
	 * 分页遍历数据
	 * @param tableName 表名
	 * @param startRowKey 起始rowkey
	 * @param pageSize 每页条数
	 * @throws IOException
	 */
	public List<Map<String,Object>> scanByPageSize(String tableName,String startRowKey,int pageSize) throws IOException{
		log.info("---------------查询 START-----------------");
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
		//根据rowkey查询
		Table table =connection.getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		//设置取值范围
		if (StringUtils.isNotEmpty(startRowKey)) {
			scan.setStartRow(Bytes.toBytes(startRowKey));//开始的key
		}
		PageFilter pageFilter = new PageFilter(pageSize);
		scan.setFilter(pageFilter);
        ResultScanner scanner = table.getScanner(scan) ;
        for (Result rs : scanner) {
        	Map<String,Object> map = new LinkedHashMap<String,Object>();
            for (Cell cell : rs.listCells()) {
            	map.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
            }
            list.add(map);
        }
		log.info("---------------查询 END-----------------");
		return list;
	}

	
}