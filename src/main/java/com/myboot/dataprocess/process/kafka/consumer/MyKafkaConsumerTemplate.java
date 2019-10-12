package com.myboot.dataprocess.process.kafka.consumer;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Component;

import com.myboot.dataprocess.process.kafka.common.MyKafkaConfiguration;

import lombok.extern.slf4j.Slf4j;

@Component
@EnableKafka
@Slf4j
public class MyKafkaConsumerTemplate {
	
	@Autowired
	private MyKafkaConfiguration myKafkaProducerConfig;
	
	@Bean
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
	    ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
	    factory.setConsumerFactory(consumerFactory());
	    log.info(myKafkaProducerConfig.getOtherParameter("concurrency"));
	    factory.setConcurrency(Integer.valueOf(myKafkaProducerConfig.getOtherParameter("concurrency")));
	    log.info(myKafkaProducerConfig.getOtherParameter("polltimeout"));
	    factory.getContainerProperties().setPollTimeout(Long.valueOf(myKafkaProducerConfig.getOtherParameter("polltimeout")));
	    return factory;
	}
	    
	    
	    public ConsumerFactory<String, String> consumerFactory() {
	        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
	    }
	    
	    public Map<String, Object> consumerConfigs() {
	        Map<String, Object> propsMap = new HashMap<>();
	        propsMap.putAll(myKafkaProducerConfig.getCommon());
	        propsMap.putAll(myKafkaProducerConfig.getConsumer());
	        return propsMap;
	    }

}
