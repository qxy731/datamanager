package com.myboot.dataprocess.process.akka;

import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import com.huateng.dinst.api.ApiResponse;
import com.huateng.dinst.api.DataModeEnum;
import com.huateng.dinst.api.DinstModeEnum;
import com.huateng.dinst.api.EvalRequestDto;
import com.huateng.dinst.api.EvalResultDto;
import com.huateng.dinst.api.ResponseModeEnum;
import com.huateng.dinst.client.bootstrap.ClientRequestTemplateByAsk;
import com.huateng.dinst.client.bootstrap.DinstClientBoot2;
import com.myboot.dataprocess.common.MyConstants;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class AkkaProcess {
    
   /* @Value("${kafka.other.hbase_zookeeper_quorum_msg}")*/
    private static String zookeeperConnectMsg = "192.168.1.39:2181,192.168.1.147:2181,192.168.1.90:2181";
    
    /*@Value("${kafka.other.akka_network_ip_startwith}")*/
    private static String AKKA_NETWORK_IP_STARTWITH_NAME ="tradErm02";
    
    private static AkkaProcess instance  = new AkkaProcess();
	
	private AkkaProcess(){
		String logpath = AkkaProcess.class.getResource("/log4j2.xml").getPath();
        //log.info("Get akka log path = " + logpath);
        try {
            //System.out.println("akka connect zookeeper msg = " + zookeeperConnectMsg + " and akka_network_ip_startwith_name = " + AKKA_NETWORK_IP_STARTWITH_NAME);
            //log.info("akka connect zookeeper msg = " + zookeeperConnectMsg + " and akka_network_ip_startwith_name = " + AKKA_NETWORK_IP_STARTWITH_NAME);
            DinstClientBoot2.loadClient(2551, logpath, zookeeperConnectMsg, "dinst-demo1", AKKA_NETWORK_IP_STARTWITH_NAME, 3551);
            //log.info("与规则引擎系统连接成功！");
            //System.out.println("与规则引擎系统连接成功！");
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	public static AkkaProcess getInstance() {
		return instance;
	}
	/*@Autowired
    private MyHbaseConfiguration myHbaseConfiguration;
	
	@Autowired
	private MyKafkaConfiguration myKafkaConfiguration;*/
    
    //向akka发送数据，返回不为null则成功
    public EvalResultDto send(Map<String, Object> reqBody, String api, String reqNo) {
        ApiResponse<EvalResultDto> response = null;
        EvalResultDto payload = null;
        try {
            Date now = new Date();
            String applicationDate = reqBody.get("ApplicationDate")==null?"":reqBody.get("ApplicationDate").toString();
            reqBody.put(MyConstants.FLAT_TRAD_DATE_TIME_NAME, applicationDate);
            reqBody.put(MyConstants.MY_APP_DATA_TYPE_NAME, "1");
            log.info("send akka : reqno = "+reqNo+"; time = "+now.getTime()+"; api = "+api);
            /*for(String key : reqBody.keySet()) {
                log.info("Send akka : key = "+key+"; value = "+reqBody.get(key));
            }*/
            EvalRequestDto evalRequestDto = new EvalRequestDto(reqNo, now.getTime(), api, reqBody, ResponseModeEnum.SLOW, DinstModeEnum.HISTORY_MODE, DataModeEnum.REAL_DATA);
            log.info("create evalRequestDto success."+evalRequestDto);
            response = ClientRequestTemplateByAsk.send(evalRequestDto);
            log.info("Get evalRequestDto = "+response);

            payload = response.getPayload();
            log.info("reqNo = " + reqNo + "返回结果成功.");
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Send to akka,get error " + e.getMessage());
        }

        return payload;
    }

    //对返回结果进行解析,形成map
    public Map<String, Map<String, Object>> getResultsFormEvalResultDto(Map<String, Object> reqBody, String api, String reqNo) {
        EvalResultDto resultDto = send(reqBody, api, reqNo);
        if (resultDto == null) {
            log.info("Get the reponse of " + reqNo + " is null.");
            return null;
        }
        Map<String, Map<String, Object>> cache = new TreeMap<>();
        Map<String, Object> resultMap = resultDto.getResultMap();
        //log.info("Get the reponse of " + reqNo + " has value.");
        for (String key : resultMap.keySet()) {
            System.out.println("Get response from akka, name = " + key + ";value = " + resultMap.get(key));
            log.info("Get response from akka, name = " + key + ";value = " + resultMap.get(key));
        }
        cache.put(reqNo, resultMap);
        return cache;
    }


    public static void main(String[] args) {
    	
    }
}
