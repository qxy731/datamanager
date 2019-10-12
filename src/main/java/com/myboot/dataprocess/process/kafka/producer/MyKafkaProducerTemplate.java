package com.myboot.dataprocess.process.kafka.producer;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

import com.myboot.dataprocess.process.kafka.common.MyKafkaConfiguration;

@Component
@EnableKafka
public class MyKafkaProducerTemplate {
	
	@Autowired
	private MyKafkaConfiguration myKafkaProducerConfig;
	
	@Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<String,String>(producerFactory());
    }
	
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }
    
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        //myKafkaProducerConfig.getCommonParameter(key)
        props.putAll(myKafkaProducerConfig.getCommon());
        props.putAll(myKafkaProducerConfig.getProducer());
        /*props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
      	props.put(ProducerConfig.RETRIES_CONFIG, retries);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        props.put(ProducerConfig.LINGER_MS_CONFIG, linger);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);*/
        return props;
    }

}
