package com.myboot.dataprocess.process.hbase.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import com.myboot.dataprocess.DataprocessApplication;

import lombok.extern.slf4j.Slf4j;
/** 
*
* @ClassName ：MyHbaseConfiguration 
* @Description ： 属性文件实体工具类
* @author ：PeterQi
*
*/
@Component
@Slf4j
@Configuration
@EnableConfigurationProperties({MyHbaseProperties.class})
public class MyHbaseConfiguration {
    
    @SuppressWarnings("unused")
	private final MyHbaseProperties myHbaseProperties;
    private final Map<String, String> config;
    private final Map<String, String> other;
    
    public MyHbaseConfiguration(MyHbaseProperties myHbaseProperties) {
        this.myHbaseProperties = myHbaseProperties;
        if(myHbaseProperties != null) {
        	this.config = myHbaseProperties.getConfig();
        	this.other = myHbaseProperties.getOther();
        }else {
        	this.config = new HashMap<String,String>();
        	this.other = new HashMap<String,String>();
        }
    }
    
    /**
     * 获取HBASE配置
     * @return
     */
    public org.apache.hadoop.conf.Configuration configuration() {
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        Set<String> keySet = config.keySet();
        for (String key : keySet) {
            configuration.set(key, config.get(key));
            log.info(key + ":" + config.get(key));
        }
        return configuration;
    }
    
    public String getOtherParameter(String key) {
    	return other.get(key);
    }
    
    public String getConfigParameter(String key) {
    	return config.get(key);
    }
    
    public static void main(String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(DataprocessApplication.class, args);
		MyHbaseProperties myHbaseProperties = context.getBean(MyHbaseProperties.class);
		if(myHbaseProperties == null) {
			log.info("myHbaseProperties is null....");
		}
		//log.info(myHbaseProperties);
		Map<String,String> config  = myHbaseProperties.getConfig();
		if(config == null) {
			log.info("config is null....");
		}
		Set<String> configKeySet = config.keySet();
        for (String key : configKeySet) {
        	log.info("=============================");
        	log.info(key + ":" + config.get(key));
        	log.info("=============================");
        }
        Map<String,String> other  = myHbaseProperties.getOther();
		if(other == null) {
			log.info("other is null....");
		}
		Set<String> otherKeySet = other.keySet();
        for (String key : otherKeySet) {
        	log.info("=============================");
        	log.info(key + ":" + other.get(key));
        	log.info("=============================");
        }
	}
}