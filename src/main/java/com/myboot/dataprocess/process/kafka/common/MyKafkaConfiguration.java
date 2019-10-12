package com.myboot.dataprocess.process.kafka.common;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import lombok.Getter;
/** 
*
* @ClassName ：MyKafkaConfiguration
* @Description ： 属性文件配置工具类
* @author ：PeterQi
*
*/
@Component
@Configuration
@EnableConfigurationProperties({MyKafkaProperties.class})
public class MyKafkaConfiguration {
	
	@SuppressWarnings("unused")
	private final MyKafkaProperties myKafkaProperties;
	
	@Getter
	private final Map<String, String> common;
	@Getter
	private final Map<String, String> producer;
	@Getter
	private final Map<String, String> consumer;
	@Getter
	private final Map<String, String> other;
    
    public MyKafkaConfiguration(MyKafkaProperties myKafkaProperties) {
        this.myKafkaProperties = myKafkaProperties;
        if(myKafkaProperties != null) {
        	this.common = myKafkaProperties.getCommon();
        	this.producer = myKafkaProperties.getProducer();
        	this.consumer = myKafkaProperties.getConsumer();
        	this.other = myKafkaProperties.getOther();
        }else {
        	this.common = new HashMap<String,String>();
        	this.producer = new HashMap<String,String>();
        	this.consumer = new HashMap<String,String>();
        	this.other = new HashMap<String,String>();
        }
    }
    
    public String getParameter(String key) {
    	String value = "";
    	value = this.getOtherParameter(key);
    	if(value.length()>0) {
    		return value;
    	}
    	value = this.getCommonParameter(key);
    	if(value.length()>0) {
    		return value;
    	}
    	value = this.getProducerParameter(key);
    	if(value.length()>0) {
    		return value;
    	}
    	value = this.getConsumerParameter(key);
    	if(value.length()>0) {
    		return value;
    	}
    	value = consumer.get(key);
    	return value;
    }
	
    public String getOtherParameter(String key) {
    	return other.get(key);
    }
    
    public String getProducerParameter(String key) {
    	return producer.get(key);
    }
    
    public String getConsumerParameter(String key) {
    	return consumer.get(key);
    }
    
    public String getCommonParameter(String key) {
    	return common.get(key);
    }

}