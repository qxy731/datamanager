package com.myboot.dataprocess.process.kafka.common;

import java.util.Map;
import java.util.Set;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import com.myboot.dataprocess.DataprocessApplication;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
/** 
*
* @ClassName ：MyKafkaProperties 
* @Description ： 属性文件配置实体类
* @author ：PeterQi
*
*/
@Slf4j
@Data
@Primary
@Component
@ConfigurationProperties(prefix = "kafka",ignoreUnknownFields=true,ignoreInvalidFields=true)
@PropertySource(value={"classpath:config/kafka.properties"},ignoreResourceNotFound=false,encoding="UTF-8",name="myKafkaProperties")
public class MyKafkaProperties {
	
	private Map<String,String> common;
	
	private Map<String,String> producer;
	
	private Map<String,String> consumer;

	private Map<String,String> other;
	
	public static void main(String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(DataprocessApplication.class, args);
		MyKafkaProperties myKafkaProperties = context.getBean(MyKafkaProperties.class);
		if(myKafkaProperties == null) {
			log.info("myKafkaProperties is null....");
		}
		log.info(myKafkaProperties.toString());
		Map<String,String> config  = myKafkaProperties.getCommon();
		if(config == null) {
			log.info("config is null....");
		}
		Set<String> configKeySet = config.keySet();
        for (String key : configKeySet) {
        	log.info(key + ":" + config.get(key));
        }
        Map<String,String> other  = myKafkaProperties.getOther();
		if(other == null) {
			log.info("other is null....");
		}
		Set<String> otherKeySet = other.keySet();
        for (String key : otherKeySet) {
        	log.info(key + ":" + other.get(key));
        }
	}
}
