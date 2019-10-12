package com.myboot.dataprocess.process.hbase.common;

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
* @ClassName ：MyHbaseProperties 
* @Description ： 属性文件实体类
* @author ：PeterQi
*
*/
@Data
@Primary
@Component
@Slf4j
@ConfigurationProperties(prefix = "hbase",ignoreUnknownFields=true,ignoreInvalidFields=true)
@PropertySource(value={"classpath:config/hbase.properties"},ignoreResourceNotFound=false,encoding="UTF-8",name="myHbaseProperties")
public class MyHbaseProperties {
	
	private Map<String,String> config;
	
	private Map<String,String> other;
	
	/*@Value("${hbase.zookeeper.quorum}")
    private String hbaseZookeeperQuorum ;
    
    @Value("${hbase.zookeeper.property.clientPort}")
    private String hbaseZookeeperPropertyClientPort;
    
    @Value("${hbase.zookeeper.znode.parent}")
    private String hbaseZookeeperZnodeParent;*/
	
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