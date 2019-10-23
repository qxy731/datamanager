package com.myboot.dataprocess.process.ignite;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.log4j2.Log4J2Logger;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.stream.StreamTransformer;

import com.google.common.collect.Maps;
import com.google.gson.Gson;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MyIgniteStreamProcess {
	
	public static void readerFileDataToIgnite(String filePath) throws Exception{
		IgniteConfiguration gridCfg = new IgniteConfiguration();
	    // cfg.setDiscoverySpi(new ZookeeperDiscoverySpi().setZkConnectionString("127.0.0.1:2181"));
	    gridCfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(
	        new TcpDiscoveryMulticastIpFinder().setAddresses(Collections.singleton("127.0.0.1:47500..47509"))));
	    String logPath = "src/main/resources/log4j2.xml";
	    gridCfg.setGridLogger(new Log4J2Logger(logPath));
	    gridCfg.setClientMode(true);
	    // 5 GB maximum size
	    // gridCfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setMaxSize(5L * 1024 * 1024 * 1024);
	    Ignite grid = Ignition.start(gridCfg);
	    CacheConfiguration<String, Object> cacheCfg = new CacheConfiguration<>("myStreamCache");
	    cacheCfg.setCopyOnRead(false);
	    //cacheCfg.setCacheMode(cacheMode)

	    //IgniteCache<String, Object> cache = grid.getOrCreateCache(cacheCfg);
	    long now = System.currentTimeMillis();
	    try (IgniteDataStreamer<String, Object> stmr = grid.dataStreamer("myStreamCache")) {
	    	stmr.allowOverwrite(true);
	    	stmr.receiver(new StreamTransformer<String, Object>() {
	    		private static final long serialVersionUID = 1L;
	    		@Override
	    		public Object process(MutableEntry<String, Object> entry, Object... arguments) throws EntryProcessorException {
	    			// System.out.println(arguments[0]);
	    			@SuppressWarnings("unchecked")
	    			Map<String, Integer> slot = (Map<String, Integer>) entry.getValue();
	    			if (slot == null) {
	    				slot = Maps.newHashMap();
	    			}
	    			// String dim = entry.getKey();
	    			//String date = String.valueOf(arguments[0]);
	    			/*slot.compute(date, (k, v) -> {
	    				v = v == null ? 0 : v + 1;
	    				return v;
	    			});*/
	    			entry.setValue(slot);
	    			return null;
	    		}
	      });
	      BufferedReader reader = null;
		  try {
			   File file = new File(filePath);
			   if (!file.isFile()) {
				   log.info("file is null");
				   return;
			   }
		   Gson gson = new Gson();
		   reader = new BufferedReader(new FileReader(file));
		   String line = "";
		   while ((line = reader.readLine()) != null) {
			    @SuppressWarnings("unchecked")
			    //entries = {##aaaa-count-his-Salescode:"4446":{ver:2,data "20190913":11, "20190914":12 }}
			    ImmutablePair<String,Object> entries = gson.fromJson(line, ImmutablePair.class);
			    stmr.addData(entries);
		   }
		  }catch (Exception e) {
			  log.error("读取文件中的数据存入ignite出错", e);
		  }finally {
			  try {
				  if (reader != null) {
					reader.close();
				  }
			  } catch (IOException e) {
				  log.error("流关闭出错！", e);
			  }
		  }
		  log.info(""+ (System.currentTimeMillis() - now));
	    }
	}
 

  public static void main(String[] args) {
   
  }
  
}