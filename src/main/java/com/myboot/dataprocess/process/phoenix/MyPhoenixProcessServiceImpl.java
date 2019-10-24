package com.myboot.dataprocess.process.phoenix;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.myboot.dataprocess.process.kafka.common.MyKafkaConfiguration;

import lombok.extern.slf4j.Slf4j;

import com.myboot.dataprocess.common.MyConstants;


@Slf4j
@Service
public class MyPhoenixProcessServiceImpl implements MyPhoenixProcessService {

    private static final int COMMIT_SIZE = 1000;

    private static int total = 0;

    private ExecutorService E1 = Executors.newSingleThreadExecutor();

    private ScheduledExecutorService E2 = Executors.newScheduledThreadPool(1);

    List<String> sqls = new LinkedList<String>();

    int cnt = 0;

    {
        E2.scheduleWithFixedDelay(() -> {
            processPhoenix(null, 1);
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
     * 向phoenix保存数据
     *
     * @throws Exception
     */
    @Override
    public void processPhoenix(Map<String, Object> mapMessage, int type) {
        E1.execute(() -> {
            try {
                if (type == 0) {
                    processPhoenixImpl(mapMessage);
                    if (++cnt < COMMIT_SIZE) {
                        return;
                    }
                }
                if (sqls.size() == 0) {
                    return;
                }
                try (Connection conn = repository.getConnection()) {
                    conn.setAutoCommit(false);
                    for (String sql : sqls) {
                        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                            total++;
                            stmt.executeUpdate();
                            log.info("MyPhoenixProcessServiceImpl#processPhoenix:send current message to phoenix " + total + "条数");
                        } catch (Exception e) {
                            log.error("MyPhoenixProcessServiceImpl#processPhoenix: send current message to phoenix is fail...");
                            log.error("MyPhoenixProcessServiceImpl#processPhoenix: " + sql);
                            log.error("MyPhoenixProcessServiceImpl#processPhoenix: " + e.getMessage());
                        }
                    }
                    conn.commit();
                } catch (Exception e) {
                } finally {
                    cnt = 0;
                    sqls.clear();
                }
            } catch (Exception e) {
                log.error("MyPhoenixProcessServiceImpl#processPhoenix: send source message to phoenix is fail...");
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
//		mapMessage.remove("MyStartDate");
//////		mapMessage.remove("MyEndDate");
//////		mapMessage.remove("MyDataFlag");
//////		mapMessage.remove("myDataFlag");
//////		mapMessage.remove("MYSTARTDATE");
//////		mapMessage.remove("MYENDDATE");
//////		mapMessage.remove("MYDATAFLAG");
//////		mapMessage.remove("FLAT_TRAD_DATE_TIME");
////////		mapMessage.remove("MYAPPDATATYPE");
//////		mapMessage.remove("MyAppDataType");
        mapMessage.remove(MyConstants.MY_START_DATE_NAME);
        mapMessage.remove(MyConstants.MY_END_DATE_NAME);
        mapMessage.remove(MyConstants.MY_DATA_FLAG_NAME);
        mapMessage.remove(MyConstants.FLAT_TRAD_DATE_TIME_NAME);
        mapMessage.remove(MyConstants.MY_APP_DATA_TYPE_NAME);

        int size = mapMessage.size();
        int count = 0;
        for (Map.Entry<String, Object> entry : mapMessage.entrySet()) {
            count++;
            String key = entry.getKey();
            Object value = entry.getValue();
            String cvalue = "";
            if (value != null) {
                cvalue = String.valueOf(value);
            }
            columns.append(key.toUpperCase());
            values.append("'").append(cvalue).append("'");
            if (count < size) {
                columns.append(",");
                values.append(",");
            }
        }
        sb.append("(").append(columns).append(")");
        sb.append(" values (").append(values).append(")");
        log.info("upsert into phoenix table sql:" + sb.toString());
//        if (sqls.size() < COMMIT_SIZE) {
        sqls.add(sb.toString());
//        }
    }

    @Override
    public Map<String, Integer> writeHisData() {
        String[] sqls = {
                "SELECT APPLICATIONDATE,SALESCODE,COUNT(1) AS COUNT FROM POCTEST3_DEST GROUP BY APPLICATIONDATE,SALESCODE",
                "SELECT APPLICATIONDATE,SALESCODE,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE APPROVALRESULT='通过' GROUP BY APPLICATIONDATE,SALESCODE",
                "SELECT APPLICATIONDATE,SALESCODE,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE ALARMCODE IN ('H','S') GROUP BY APPLICATIONDATE,SALESCODE",
                "SELECT APPLICATIONDATE,SALESCODE,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE RESEARCHCONCLUSION='K' GROUP BY APPLICATIONDATE,SALESCODE",
                "SELECT APPLICATIONDATE,SALESCODE,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE RESEARCHCONCLUSION IN('K','F') GROUP BY APPLICATIONDATE,SALESCODE",
                "SELECT APPLICATIONDATE,COUNT(1) AS COUNT FROM POCTEST3_DEST GROUP BY APPLICATIONDATE",
                "SELECT APPLICATIONDATE,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE APPROVALRESULT='通过' GROUP BY APPLICATIONDATE",
                "SELECT APPLICATIONDATE,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE ALARMCODE IN ('H','S') GROUP BY APPLICATIONDATE ",
                "SELECT APPLICATIONDATE,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE RESEARCHCONCLUSION='K' GROUP BY APPLICATIONDATE ",
                "SELECT APPLICATIONDATE,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE RESEARCHCONCLUSION IN('K','F') GROUP BY APPLICATIONDATE ",
                "SELECT APPLICATIONDATE,COMPANYPHONENUMBER,COUNT(1) AS COUNT FROM POCTEST3_DEST GROUP BY APPLICATIONDATE,COMPANYPHONENUMBER",
                "SELECT APPLICATIONDATE,COMPANYPHONENUMBER,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE RESEARCHCONCLUSION='K' GROUP BY APPLICATIONDATE,COMPANYPHONENUMBER",
                "SELECT APPLICATIONDATE,COMPANYPHONENUMBER,COMPANYNAME,COUNT(1) AS COUNT FROM POCTEST3_DEST GROUP BY APPLICATIONDATE,COMPANYPHONENUMBER,COMPANYNAME",
                "SELECT APPLICATIONDATE,COMPANYNAME,COUNT(1) AS COUNT FROM POCTEST3_DEST GROUP BY APPLICATIONDATE,COMPANYNAME",
                "SELECT APPLICATIONDATE,COMPANYNAME,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE RESEARCHCONCLUSION='K' GROUP BY APPLICATIONDATE,COMPANYNAME",
                "SELECT APPLICATIONDATE,IP,COUNT(1) AS COUNT FROM POCTEST3_DEST GROUP BY APPLICATIONDATE,IP",
                "SELECT APPLICATIONDATE,IP,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE RESEARCHCONCLUSION='K' GROUP BY APPLICATIONDATE,IP",
                "SELECT APPLICATIONDATE,IP,COMPANYPHONENUMBER,COUNT(1) AS COUNT FROM POCTEST3_DEST GROUP BY APPLICATIONDATE,IP,COMPANYPHONENUMBER",
                "SELECT APPLICATIONDATE,APPID,COUNT(1) AS COUNT FROM POCTEST3_DEST GROUP BY APPLICATIONDATE,APPID",
                "SELECT APPLICATIONDATE,APPID,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE RESEARCHCONCLUSION='K' GROUP BY APPLICATIONDATE,APPID",
                "SELECT APPLICATIONDATE,COOKIE,COUNT(1) AS COUNT FROM POCTEST3_DEST GROUP BY APPLICATIONDATE,COOKIE",
                "SELECT APPLICATIONDATE,COOKIE,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE RESEARCHCONCLUSION='K' GROUP BY APPLICATIONDATE,COOKIE"
        };
        String rootPath = "/hdata/poctest3/file/";
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (int i = 0; i < sqls.length; i++) {
            String sql = sqls[i];
            String filePath = rootPath + "file" + i + ".txt";
            int total = writeHisMessage(sql, filePath);
            map.put(filePath, total);
        }
        return map;
    }

    @Override
    public Map<String, Integer> writeHisDataBySql(List<String> sqls) {
        String rootPath = "/hdata/poctest3/file/";
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (int i = 0; i < sqls.size(); i++) {
            String sql = sqls.get(i);
            String filePath = rootPath + "file" + i + ".txt";
            int total = writeHisMessage(sql, filePath);
            map.put(filePath, total);
        }
        return map;
    }

    @Override
    public Map<String, Integer> writeHisDataByFile(Map<String, String> sqls) {
        String rootPath = "/hdata/poctest3/file/";
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (Map.Entry<String, String> sqlObject : sqls.entrySet()) {
            String filename = sqlObject.getKey();
            String sql = sqlObject.getValue();
            String filePath = rootPath + filename + ".txt";
            int total = writeHisMessage(sql, filePath);
            map.put(filePath, total);
        }
        return map;
    }

    /**
     * 从phoenix源表中读取1000W数据推送到kafka
     *
     * @throws Exception
     */
    public int writeHisMessage(String sql, String filename) {
        int total = 0;
        PrintWriter out = null;
        try (Connection conn = repository.getConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                //写入相应的文件
                out = new PrintWriter(new FileWriter(filename));
                ResultSet rs = stmt.executeQuery(sql);
                long start = System.currentTimeMillis();
                while (rs.next()) {
                    total++;
                    ResultSetMetaData meta = rs.getMetaData();
                    int length = meta.getColumnCount();
                    Map<String, Object> map = new HashMap<String, Object>();
                    for (int i = 1; i <= length; i++) {
                        String column = meta.getColumnLabel(i);
                        String value = rs.getString(i);
                        map.put(column, value);
                    }
                    Gson gson = new Gson();
                    String jsonStr = gson.toJson(map);
                    out.write(jsonStr);
                    out.write(System.getProperty("line.separator"));
                    log.info("MyKafkaProducerPhoenixServiceImpl#writeHisMessage#outwrite:" + filename + "&" + total);
                    //清楚缓存
                    if (total % 50000 == 0) {
                        out.flush();
                        log.info("MyKafkaProducerPhoenixServiceImpl#writeHisMessage#outflush:" + filename + "&" + total + " cost:" + (System.currentTimeMillis() - start) + "ms");
                    }
                }
                log.info("MyKafkaProducerPhoenixServiceImpl#writeHisMessage:" + filename + "&" + total + " cost:" + (System.currentTimeMillis() - start) + "ms");
            }
        } catch (Exception e) {
            log.info("MyKafkaProducerPhoenixServiceImpl#writeHisMessage throw excepton ...");
            log.error(e.getMessage());
        } finally {
            try {
                if (out != null) {
                    out.flush();
                    //关闭流
                    out.close();
                }
            } catch (Exception e) {

            }
        }
        return total;
    }

    public static void main(String[] args) {
		/*StringBuffer sb = new StringBuffer("upsert into ");
		sb.append("POCTEST3_DEST");
		Map<String, Object> mapMessage  =  new HashMap<String,Object>();
		mapMessage.put("FLAT_TRAD_DATE_TIME", "13455");
		mapMessage.put("MYSTARTDATE", "2222222222222222");
		mapMessage.put("MYENDDATE", "33333333333333333");
		int size = mapMessage.size();
		mapMessage.remove("FLAT_TRAD_DATE_TIME");
		int count = 0;
		StringBuffer columns = new StringBuffer();
		StringBuffer values = new StringBuffer();
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
		}
		sb.append("(").append(columns).append(")");
		sb.append(" values (").append(values).append(")");
		log.info("upsert into phoenix table sql:"+sb.toString());*/
        //PrintWriter out = null;
        //writeHisData();
        System.out.println(System.getProperty("line.separator"));
    }

}