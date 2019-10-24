package com.myboot.dataprocess.process.kafka.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.myboot.dataprocess.common.MyConstants;
import com.myboot.dataprocess.model.KafkaApplyCardEntity;
import com.myboot.dataprocess.process.akka.MyAkkaProcessService;
import com.myboot.dataprocess.process.kafka.common.KafkaDataModelProcess;
import com.myboot.dataprocess.process.phoenix.MyPhoenixProcessService;
import com.myboot.dataprocess.tools.CommonTool;

import lombok.extern.slf4j.Slf4j;

/**
 *  
 *
 * @author ：PeterQi
 * @ClassName ：MyKafkaCustermer 
 * @Description ： 消费者处理类
 */
@Slf4j
@Service
/*@Scope("prototype")*/
public class MyKafkaConsumerServiceImpl implements MyKafkaConsumerService {

    @Autowired
    private MyAkkaProcessService akkaService;

    @Autowired
    private MyPhoenixProcessService phoenixService;

    private static int sourceCount = 0;

    private static long sourceStartTime = 0;

    private static long sourceEndTime = 0;

    private static int resultCount = 0;

    private static long resultStartTime = 0;

    private static long resultEndTime = 0;

    private ScheduledExecutorService E2 = Executors.newScheduledThreadPool(1);

    {

        try {
            E2.scheduleWithFixedDelay(() -> {
                if (sourceCount != 0) {
                    log.info("目前从kafka源topic已消费" + sourceCount + "条数据。总共耗时" + (sourceEndTime - sourceStartTime) + "ms,平均耗时：" + (sourceEndTime - sourceStartTime) / sourceCount + "ms");
                }
                if (resultCount != 0) {
                    log.info("目前从kafka结果topic已消费" + resultCount + "条数据。总共耗时" + (resultEndTime - resultStartTime) + "ms,平均耗时：" + (resultEndTime - resultStartTime) / resultCount + "ms");
                }
            }, 5L, 3L, TimeUnit.SECONDS);
        } catch (Exception e) {

        }
    }

    @Autowired
    private KafkaDataModelProcess kafkaDataModelProcess;

    @KafkaListener(topics = {"${kafka.other.source.topic}"})
    public void listener(String content) {
        log.info("==================MyKafkaCustermer listener start =========================");
        try {
            if (content != null && content.length() > 0) {
                sourceCount++;
                log.info("MyKafkaConsumerServiceImpl#listener:receive " + sourceCount + " source kafka message :" + content);
                long start = System.currentTimeMillis();
                if (sourceCount == 1) {
                    sourceStartTime = start;
                }
                Map<String, Object> mapMessage = processContent(content);
                if (mapMessage == null || mapMessage.size() == 0) {
                    return;
                }
                /**第一步：向akka传送数据并回写Kafka*/
                akkaService.processAkka(mapMessage);
                /**第二步：向phoenix保存数据*/
 				String myDataFlag = mapMessage.get(MyConstants.MY_DATA_FLAG_NAME)==null?MyConstants.MY_DATA_FLAG_REAL:mapMessage.get(MyConstants.MY_DATA_FLAG_NAME).toString();
 				if(MyConstants.MY_DATA_FLAG_REAL.equals(myDataFlag)) {
 					 phoenixService.processPhoenix(mapMessage,0);
 				}
                //phoenixService.processPhoenix(mapMessage, 0);
                long end = System.currentTimeMillis();
                log.info("MyKafkaConsumerServiceImpl#listener:receive " +sourceCount+" source kafka message consume total cost :" + (end - start) +"ms");
                sourceEndTime = end;
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        log.info("==================MyKafkaCustermer listener end ========================");
    }

 /*   @KafkaListener(topics = {"${kafka.other.dest.topic}"})*/
    public void listenerResult(String content) {
        resultCount++;
        if (resultCount == 1) {
            resultStartTime = System.currentTimeMillis();
        }
        log.info("MyKafkaConsumerServiceImpl#listenerResult:receive " + resultCount + " result topic kafka message ");
        long end = System.currentTimeMillis();
        //log.info("MyKafkaConsumerServiceImpl#listener:receive " +sourceCount+" source kafka message consume total cost :" + (end - start) +"ms");
        resultEndTime = end;
    }

    /**
     * 把接收的消息转成Map
     *
     * @param content
     * @return
     */
    private Map<String, Object> processContent(String content) {
        Gson gson = new Gson();
        Map<String, Object> map = gson.fromJson(content, new TypeToken<HashMap<String, Object>>() {
        }.getType());
        if (map == null) {
            return null;
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> dataObject = (Map<String, Object>) map.get("data");
        return dataObject;
    }


    @Override
    public void listenerTest(int myDataFlag, int count) {
        new Thread() {
            public void run() {
                String content = "";
                for (int i = 0; i < count; i++) {
                    String currentDate = CommonTool.getCurrentDate();
                    KafkaApplyCardEntity entity = kafkaDataModelProcess.assembleKafkaData(currentDate);
                    Map<String, Object> mapMessage = new HashMap<String, Object>();
                    mapMessage = entity.getData();
                    if (myDataFlag == Integer.valueOf(MyConstants.MY_DATA_FLAG_HIS)) {
                        //历史造数
                        mapMessage.put(MyConstants.MY_START_DATE_NAME, MyConstants.MY_START_DATE_DEFAULT);
                        mapMessage.put(MyConstants.MY_END_DATE_NAME, MyConstants.MY_END_DATE_DEFAULT);
                        mapMessage.put(MyConstants.MY_DATA_FLAG_NAME, MyConstants.MY_DATA_FLAG_HIS);
                        mapMessage.put(MyConstants.FLAT_TRAD_DATE_TIME_NAME,MyConstants.FLAT_TRAD_DATE_TIME_DEFAULT);
                        mapMessage.put(MyConstants.MY_APP_DATA_TYPE_NAME,MyConstants.MY_APP_DATA_TYPE_DEFAULT);
                    } else {
                        //实时
                        mapMessage.put(MyConstants.MY_START_DATE_NAME,"");
                        mapMessage.put(MyConstants.MY_END_DATE_NAME,"");
                        mapMessage.put(MyConstants.MY_DATA_FLAG_NAME,MyConstants.MY_DATA_FLAG_REAL);
                        mapMessage.put(MyConstants.FLAT_TRAD_DATE_TIME_NAME,MyConstants.FLAT_TRAD_DATE_TIME_DEFAULT);
                        mapMessage.put(MyConstants.MY_APP_DATA_TYPE_NAME,MyConstants.MY_APP_DATA_TYPE_DEFAULT);
                    }
                    entity.setData(mapMessage);
                    Gson gson = new Gson();
                    content = gson.toJson(entity);
                    listener(content);
                }
            }
        }.start();
    }

}